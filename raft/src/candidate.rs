// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, Ordering}, Arc};

use labrpc::{client::ClientEnd, err::{DISCONNECTED, TIMEOUT}};
use serde::{Deserialize, Serialize};

use crate::{common, event::{Event, TO_LEADER}, follower::Follower, log::Logs, raft::RaftCore, role::{RoleEvQueue, Trans}, service::{AppendEntriesArgs, AppendEntriesReply, EntryStatus, QueryEntryArgs, QueryEntryReply, RequestVoteArgs, RequestVoteReply, RequestVoteRes}, OneTx};
use crate::info;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VoteStatus {
    Granted,
    // Rejected is given by nodes who have voted for 
    // candidates at least as new as this candidate.
    // And the remote node may return some help information 
    // to help the candidate update itself.
    Rejected {
        term: usize
    },
    // Denied is given by a new leader, it tells 
    // the candidate to stop its election and become 
    // a follower ASAP.
    Denied {
        term: usize
    }
}

struct Poll {
    me: usize,
    term: usize,
    ev_q: RoleEvQueue
}

pub struct Candidate {
    pub core: RaftCore,
    pub logs: Logs,
    pub ev_q: RoleEvQueue,
    active: Arc<AtomicBool>,
    voters: Vec<bool>,
    votes: usize,
    n: usize
}

impl Candidate {
    pub async fn from_follower(flw: Follower) -> Self {
        let Follower {
            mut core, logs, ev_q, ..
        } = flw;
        let ev_q = ev_q.transfer();
        let n = core.rpc_client.n();
        core.term += 1;

        {
            let poll = Poll {
                me: core.me,
                term: core.term,
                ev_q: ev_q.clone()
            };
            let poll = Arc::new(poll);

            let args = Arc::new(RequestVoteArgs {
                from: core.me,
                term: core.term,
                last_log: logs.last_log_info().await
            });
            
            for peer in core.rpc_client.peers() {
                tokio::spawn(poll.clone().poll(peer, args.clone()));
            }

            let ev_q = ev_q.clone();
            tokio::spawn(async move {
                tokio::time::sleep(
                    common::gen_rand_duration(common::ELECTION_TIMEOUT)
                ).await;
                if let Ok(_) = ev_q.put(Event::ElectionTimeout).await {
                    info!("Candidate {} election timeout", core.me);
                }
            });
        }

        Self {
            core,
            logs,
            active: Arc::new(AtomicBool::new(true)),
            ev_q,
            voters: vec![false; n],
            votes: 0,
            n
        }
    }

    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        match ev {
            Event::Trans(Trans::ToCandidate) => 
                panic!("Candidate receives a Trans::ToCandidate"),
            Event::Trans(to) => return Some(to),

            // RPC request events
            Event::GetState(tx) => {
                let term = self.core.term;
                tx.send((term, false)).unwrap();
            },

            Event::StartCmd { reply_tx, .. } => {
                let _ = reply_tx.send(None);
            },

            Event::AppendEntries {args, reply_tx} => {
                info!("{self}: AppendEntries from {}, term={}", args.from, args.term);
                self.append_entries(args, reply_tx).await;
            },
            Event::RequestVote {args, reply_tx} => {
                info!("{self}: RequestVote from {}, term={}", args.from, args.term);
                self.request_vote(args, reply_tx).await;
            },
            Event::QueryEntry {args, reply_tx} => {
                info!("{self}: QueryEntry, log_info = {}", args.log_info);
                self.query_entry(args, reply_tx).await;
            }

            // candidate related events
            Event::GrantVote { voter } => {
                self.audit_vote(voter).await;
            },

            Event::OutdateCandidate {new_term} => {
                let _ = self.ev_q.put(Event::Trans(
                        Trans::ToFollower {new_term: Some(new_term)})).await;
            },
            
            Event::ElectionTimeout => {
                info!("{self} election timeout");
                let _ = self.ev_q.put(Event::Trans(Trans::ToFollower {
                    new_term: None
                })).await;
            }

            ev => panic!("Unexpected event for a candidate: {ev}")
        }
        None
    }

    pub async fn stop(&self) {
        self.active.store(false, Ordering::Release);
    }

    /// When a candidate receives a AppendEntries request, it first checks
    /// the request term, if it's at least as new as itself, means there's 
    /// a new leader selected, then the candidate fallback to be a follower.
    /// Otherwise, it's a request from a outdated leader, so the request is 
    /// rejected, candidate returns its new term instead.
    async fn append_entries(&mut self, args: AppendEntriesArgs,
        reply_tx: OneTx<AppendEntriesReply>) {
        let myterm = self.core.term;
        
        let entry_status = if args.term >= myterm {
            let _ = self.ev_q.put(Event::Trans(Trans::ToFollower {
                new_term: (args.term > myterm).then(|| args.term)
            })).await;
            EntryStatus::Retry
        } else {
            EntryStatus::Stale {
                term: myterm
            }
        };
        let reply = AppendEntriesReply {
            from: self.core.me,
            entry_status
        };
        reply_tx.send(reply).unwrap();
    }

    /// When a candidate receivges a RequestVote request, it must come from 
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
    async fn request_vote(&mut self, args: RequestVoteArgs, reply_tx: 
        OneTx<RequestVoteReply>) {
        let myterm = self.core.term;

        use std::cmp;
        let vote = match myterm.cmp(&args.term) {
            cmp::Ordering::Greater => {
                VoteStatus::Rejected { term: myterm }
            },
            cmp::Ordering::Equal => {
                match &self.core.vote_for {
                    Some(vote_for) if *vote_for == args.from => VoteStatus::Granted,
                    _ => VoteStatus::Rejected { term: myterm }
                }
            },
            cmp::Ordering::Less => {
                let _ = self.ev_q.put(Event::Trans(Trans::ToFollower {
                    new_term: Some(args.term)
                })).await;
                let up_to_date = self.logs.up_to_date(&args.last_log).await;

                if up_to_date {
                    self.core.vote_for = Some(args.from);
                    VoteStatus::Granted
                } else {
                    {
                        let my_last = self.logs.last_log_info().await;
                        info!("Reject {} for stale logs, my last = {}, its last = {}", args.from, my_last, args.last_log);
                        
                    }
                    VoteStatus::Rejected {
                        term: myterm
                    }
                }
            }
        };
        
        let reply = RequestVoteReply {
            voter: self.core.me,
            vote
        };
        reply_tx.send(reply).unwrap();
    }

    pub async fn query_entry(&self, args: QueryEntryArgs, reply_tx: OneTx<QueryEntryReply>) {
        let reply = if self.logs.log_exist(&args.log_info).await {
            QueryEntryReply::Exist
        } else { QueryEntryReply::NotExist };
        reply_tx.send(reply).unwrap();
    }

    async fn audit_vote(&mut self, voter: usize) {
        assert!(voter != self.core.me && voter < self.n, 
            "Invalid voter id {voter}");

        if !self.voters[voter] {
            self.voters[voter] = true;
            self.votes += 1;
            if self.votes >= self.n / 2 {
                let _ = self.ev_q.put(TO_LEADER).await;
            }
        }
    }
}

impl Poll {
    async fn poll(self: Arc<Self>, peer: ClientEnd, 
        args: Arc<RequestVoteArgs>) {
        const TRIES: usize = 5;
        let mut tries = 0usize;

        let ret = loop {
            let ret = peer.call::<_, RequestVoteRes>(
                crate::common::REQUEST_VOTE,
                args.as_ref()
            ).await;
            
            match ret {
                Ok(Ok(r)) => break Some(r),
                Ok(Err(_)) | Err(DISCONNECTED) => break None,
                Err(TIMEOUT) => {
                    if tries == TRIES {
                        break None;
                    }
                },
                Err(e) => panic!("Unexpect Error: {e:?}")
            }
            tries += 1;
            tokio::time::sleep(
                crate::common::RPC_RETRY_WAIT
            ).await;
        };

        if let Some(reply) = ret {
            self.check_vote(reply).await;
        }
    }

    async fn check_vote(&self, reply: RequestVoteReply) {
        let voter = reply.voter;
        info!("{self} vote reply from {voter}, vote {}", reply.vote);
        match reply.vote {
            VoteStatus::Granted => {
                let _ = self.ev_q.put(Event::GrantVote {
                    voter: reply.voter,
                }).await;
            },
            VoteStatus::Denied { term } => {
                let myterm = self.term;
                if myterm <= term {
                    let _ = self.ev_q.put(Event::OutdateCandidate {
                        new_term: term
                    }).await;
                }
            },
            VoteStatus::Rejected { term } => {
                let myterm = self.term;
                if myterm < term {
                    let _ = self.ev_q.put(Event::OutdateCandidate {
                        new_term: term
                    }).await;
                }
            }
        }
    }
}

impl std::fmt::Display for Candidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Candidate[{}, term={}]", self.core.me, self.core.term)
    }
}

impl std::fmt::Display for Poll {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Candidate[{}, term={}]", self.me, self.term)
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
