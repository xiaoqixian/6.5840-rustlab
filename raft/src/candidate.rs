// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use labrpc::{
    client::ClientEnd,
    err::{DISCONNECTED, TIMEOUT},
};
use serde::{Deserialize, Serialize};

use crate::info;
use crate::{
    common::{self, RPC_RETRY_WAIT},
    debug,
    event::{Event, TO_FOLLOWER, TO_LEADER},
    logs::Logs,
    raft::RaftCore,
    role::{RoleCore, RoleEvQueue, Trans},
    service::{
        AppendEntriesArgs, AppendEntriesReply,
        EntryStatus, QueryEntryArgs,
        QueryEntryReply, RequestVoteArgs,
        RequestVoteReply, RequestVoteRes,
    },
    warn, OneTx,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VoteStatus {
    Granted,
    // Rejected is given by nodes who have voted for
    // candidates at least as new as this candidate.
    // And the remote node may return some help information
    // to help the candidate update itself.
    Rejected { term: usize },
    // Denied is given by a new leader, it tells
    // the candidate to stop its election and become
    // a follower ASAP.
    Denied { term: usize },
}

struct Poll {
    me: usize,
    term: usize,
    active: Arc<AtomicBool>,
    ev_q: RoleEvQueue,
}

/// Test 3C: A Candidate does not persist its votes data.
/// When it crashes and restarts, it lost all its votes,
/// which is fine, it starts as a follower, and followers who
/// vote for it can keep working.
pub struct Candidate {
    core: RaftCore,
    logs: Logs,
    ev_q: RoleEvQueue,
    active: Arc<AtomicBool>,
    voters: Vec<bool>,
    votes: usize,
    n: usize,
}

impl Candidate {
    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        match ev {
            Event::Trans(Trans::ToCandidate) => panic!("Candidate receives a Trans::ToCandidate"),
            Event::Trans(to) => return Some(to),

            // RPC request events
            Event::GetState(tx) => {
                let term = self.core.term;
                tx.send((term, false)).unwrap();
            }

            Event::StartCmd { reply_tx, .. } => {
                let _ = reply_tx.send(None);
            }

            Event::AppendEntries { args, reply_tx } => {
                debug!(
                    "{self}: AppendEntries from {}, term={}",
                    args.from, args.term
                );
                self.append_entries(args, reply_tx).await;
            }
            Event::RequestVote { args, reply_tx } => {
                debug!("{self}: RequestVote from {}, term={}", args.from, args.term);
                self.request_vote(args, reply_tx).await;
            }
            Event::QueryEntry { args, reply_tx } => {
                debug!("{self}: QueryEntry, log_info = {}", args.log_info);
                self.query_entry(args, reply_tx).await;
            }

            // candidate related events
            Event::GrantVote { term, voter } => {
                debug!("{self}: GrantVote from {voter}, term = {term}");
                if term == self.core.term {
                    debug!("{self}: accept vote from {voter}, term = {term}");
                    self.audit_vote(voter).await;
                }
            }

            Event::OutdateCandidate { new_term } => {
                if new_term >= self.core.term {
                    self.core.term = new_term;
                    let _ = self.ev_q.put(TO_FOLLOWER);
                    self.persist_state();
                }
            }

            Event::ElectionTimeout => {
                info!("{self} election timeout");
                let _ = self.ev_q.put(TO_FOLLOWER);
            }

            ev => panic!("Unexpected event for a candidate: {ev}"),
        }
        None
    }

    pub fn stop(self) -> RoleCore {
        self.active.store(false, Ordering::Relaxed);
        self.into()
    }

    /// When a candidate receives a AppendEntries request, it first checks
    /// the request term, if it's at least as new as itself, means there's
    /// a new leader selected, then the candidate fallback to be a follower.
    /// Otherwise, it's a request from a outdated leader, so the request is
    /// rejected, candidate returns its new term instead.
    async fn append_entries(
        &mut self,
        args: AppendEntriesArgs,
        reply_tx: OneTx<AppendEntriesReply>,
    ) {
        let myterm = self.core.term;

        let entry_status = if args.term >= myterm {
            self.core.term = args.term;
            let _ = self.ev_q.put(TO_FOLLOWER);
            self.persist_state();
            EntryStatus::Hold
        } else {
            EntryStatus::Stale { term: myterm }
        };
        let reply = AppendEntriesReply {
            from: self.core.me,
            entry_status,
        };
        reply_tx.send(reply).unwrap();
    }

    /// When a candidate receives a RequestVote request, it must come from
    /// its competitors.
    /// If there is a competitor with a newer term, the
    /// candidate fallback to be a follower, but should it vote for the
    /// competitor depends on whether the competitor has the at least new
    /// logs as the candidate.
    /// If the term is not greater than the one the candidate has, then the
    /// candidate rejects the request with VoteStatus::Rejected, which means
    /// this node has voted for someone else, which of course the candidate
    /// has voted for itself.
    /// But there is an exception, if the request has the same term, and this
    /// node has voted for the id before, then it reply VoteStatus::Granted
    /// again.
    async fn request_vote(&mut self, args: RequestVoteArgs, reply_tx: OneTx<RequestVoteReply>) {
        let myterm = self.core.term;

        use std::cmp;
        let vote = match myterm.cmp(&args.term) {
            cmp::Ordering::Greater => {
                debug!(
                    "{self}: reject vote from {} for stale term {}",
                    args.from, args.term
                );
                VoteStatus::Rejected { term: myterm }
            }
            cmp::Ordering::Equal => match &self.core.vote_for {
                Some(vote_for) if *vote_for == args.from => VoteStatus::Granted,
                _ => VoteStatus::Rejected { term: myterm },
            },
            cmp::Ordering::Less => {
                self.core.term = args.term;
                let _ = self.ev_q.put(TO_FOLLOWER);
                let up_to_date = self.logs.up_to_date(&args.last_log);

                let vote = if up_to_date {
                    self.core.vote_for = Some(args.from);
                    debug!("{self}: grant vote to {}", args.from);
                    VoteStatus::Granted
                } else {
                    {
                        debug!(
                            "Reject {} for stale logs, my last = {}, \
                            its last = {}",
                            args.from,
                            self.logs.last_log_info(),
                            args.last_log
                        );
                    }
                    VoteStatus::Rejected { term: myterm }
                };
                // if persistence failed, discard the vote, let the
                // requester timeout.
                if !self.persist_state() {
                    return;
                }
                vote
            }
        };

        let reply = RequestVoteReply {
            voter: self.core.me,
            vote,
        };
        reply_tx.send(reply).unwrap();
    }

    pub async fn query_entry(&self, args: QueryEntryArgs, reply_tx: OneTx<QueryEntryReply>) {
        let reply = if self.logs.log_exist(&args.log_info) {
            QueryEntryReply::Exist
        } else {
            QueryEntryReply::NotExist
        };
        reply_tx.send(reply).unwrap();
    }

    async fn audit_vote(&mut self, voter: usize) {
        assert!(
            voter != self.core.me && voter < self.n,
            "Invalid voter id {voter}"
        );

        if !self.voters[voter] {
            self.voters[voter] = true;
            self.votes += 1;
            debug!("{self}: accepted vote from {voter}");
            if self.votes > self.n / 2 {
                debug!("{self}: get enough votes.");
                if let Err(_) = self.ev_q.put(TO_LEADER) {
                    warn!("{self}: try to put TO_LEADER failed");
                }
            }
        }
    }

    fn persist_state(&self) -> bool {
        let state = bincode::serialize(self).unwrap();
        self.core.persister.save(Some(state), None, false)
    }
}

impl Poll {
    async fn poll(self: Arc<Self>, peer: ClientEnd, args: Arc<RequestVoteArgs>) {
        let ret = loop {
            if !self.active.load(Ordering::Relaxed) {
                return;
            }

            let req_vote =
                peer.call::<_, RequestVoteRes>(crate::common::REQUEST_VOTE, args.as_ref());
            let ret = match tokio::time::timeout(RPC_RETRY_WAIT, req_vote).await {
                Ok(r) => r,
                Err(_) => Err(TIMEOUT),
            };

            match ret {
                Ok(Ok(r)) => break Some(r),
                Ok(Err(_)) | Err(DISCONNECTED) => break None,
                Err(TIMEOUT) => {}
                Err(e) => panic!("Unexpect Error: {e:?}"),
            }
        };

        if let Some(reply) = ret {
            self.check_vote(reply).await;
        }
    }

    async fn check_vote(&self, reply: RequestVoteReply) {
        debug!(
            "{self} vote reply from {}, vote {}",
            reply.voter, reply.vote
        );
        match reply.vote {
            VoteStatus::Granted => {
                let _ = self.ev_q.put(Event::GrantVote {
                    term: self.term,
                    voter: reply.voter,
                });
            }
            VoteStatus::Denied { term } => {
                let myterm = self.term;
                if myterm <= term {
                    let _ = self.ev_q.put(Event::OutdateCandidate { new_term: term });
                }
            }
            VoteStatus::Rejected { term } => {
                let myterm = self.term;
                if myterm < term {
                    let _ = self.ev_q.put(Event::OutdateCandidate { new_term: term });
                }
            }
        }
    }
}

impl From<RoleCore> for Candidate {
    fn from(role_core: RoleCore) -> Self {
        let RoleCore {
            raft_core: mut core,
            logs,
            ev_q,
        } = role_core;
        let n = core.rpc_client.n();
        core.term += 1;

        debug!("{core}: become a candidate");

        let active = Arc::new(AtomicBool::new(true));
        {
            let poll = Poll {
                me: core.me,
                term: core.term,
                active: active.clone(),
                ev_q: ev_q.clone(),
            };
            let poll = Arc::new(poll);

            let args = Arc::new(RequestVoteArgs {
                from: core.me,
                term: core.term,
                last_log: logs.last_log_info(),
            });

            for peer in core.rpc_client.peers() {
                tokio::spawn(poll.clone().poll(peer, args.clone()));
            }

            let ev_q = ev_q.clone();
            tokio::spawn(async move {
                tokio::time::sleep(common::gen_rand_duration(common::ELECTION_TIMEOUT)).await;
                if let Ok(_) = ev_q.put(Event::ElectionTimeout) {
                    info!("Candidate {} election timeout", core.me);
                }
            });
        }

        Self {
            core,
            logs,
            active,
            ev_q,
            voters: vec![false; n],
            votes: 1,
            n,
        }
    }
}

impl Into<RoleCore> for Candidate {
    fn into(self) -> RoleCore {
        RoleCore {
            raft_core: self.core,
            logs: self.logs,
            ev_q: self.ev_q.transfer(),
        }
    }
}

impl std::fmt::Display for Candidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Candidate[{}, term={}, last_log={}, votes={}]",
            self.core.me,
            self.core.term,
            self.logs.last_log_info(),
            self.votes
        )
    }
}

impl std::fmt::Display for Poll {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Poll[{}, term={}]", self.me, self.term)
    }
}

impl std::fmt::Display for VoteStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Granted => write!(f, "Granted"),
            Self::Denied { term } => write!(f, "Denied {{term = {term}}}"),
            Self::Rejected { term } => write!(f, "Rejected {{term = {term}}}"),
        }
    }
}

impl Serialize for Candidate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("RaftState", 2)?;
        s.serialize_field("raft_info", &self.core)?;
        s.serialize_field("logs_info", &self.logs)?;
        s.end()
    }
}
