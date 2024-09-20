// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc};

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
    core: RaftCore,
    logs: Logs,
    votes: AtomicUsize,
    voters: Vec<AtomicBool>,
    quorum: usize,
    ev_q: RoleEvQueue
}

#[derive(Clone)]
pub struct Candidate {
    pub core: RaftCore,
    pub logs: Logs,
    pub ev_q: RoleEvQueue,
    active: Arc<AtomicBool>,
    poll: Arc<Poll>,
}

impl From<Follower> for Candidate {
    fn from(flw: Follower) -> Self {
        let Follower {
            core, logs, ev_q, ..
        } = flw;
        let ev_q = ev_q.transfer();
        let n = core.rpc_client.n();

        let poll = Poll {
            core: core.clone(),
            logs: logs.clone(),
            votes: AtomicUsize::new(0),
            voters: std::iter::repeat_with(|| AtomicBool::default())
                .take(n).collect(),
            // my own vote is not included
            quorum: n/2,
            ev_q: ev_q.clone()
        };
        let poll = Arc::new(poll);

        // start election
        tokio::spawn(poll.clone().start());

        Self {
            core,
            logs,
            active: Arc::new(AtomicBool::new(true)),
            poll,
            ev_q
        }
    }
}

impl Candidate {
    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        match ev {
            Event::Trans(Trans::ToCandidate) => 
                panic!("Candidate receives a Trans::ToCandidate"),
            Event::Trans(to) => return Some(to),

            // RPC request events
            Event::GetState(tx) => {
                let term = self.core.term();
                tx.send((term, false)).unwrap();
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
    async fn append_entries(&self, args: AppendEntriesArgs,
        reply_tx: OneTx<AppendEntriesReply>) {
        let myterm = self.core.term();
        
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
    async fn request_vote(&self, args: RequestVoteArgs, reply_tx: 
        OneTx<RequestVoteReply>) {
        let myterm = self.core.term();

        use std::cmp;
        let vote = match myterm.cmp(&args.term) {
            cmp::Ordering::Less => {
                VoteStatus::Rejected { term: myterm }
            },
            cmp::Ordering::Equal => {
                let vote_for = self.core.vote_for.lock().await.clone();
                match vote_for {
                    Some(vote_for) if vote_for == args.from => VoteStatus::Granted,
                    _ => VoteStatus::Rejected { term: myterm }
                }
            },
            cmp::Ordering::Greater => {
                let _ = self.ev_q.put(Event::Trans(Trans::ToFollower {
                    new_term: Some(args.term)
                })).await;
                let up_to_date = self.logs.up_to_date(&args.last_log).await;

                if up_to_date {
                    *self.core.vote_for.lock().await = Some(args.from);
                    VoteStatus::Granted
                } else {
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

    async fn audit_vote(&self, voter: usize) {
        assert!(voter != self.core.me && voter < self.core.rpc_client.n(), 
            "Invalid voter id {voter}");
        let voted = self.poll.voters[voter].swap(true, Ordering::AcqRel);
        if !voted {
            let votes = self.poll.votes.fetch_add(1, Ordering::AcqRel) + 1;
            if votes >= self.poll.quorum {
                let _ = self.ev_q.put(TO_LEADER).await;
            }
        }
    }
}

impl Poll {
    async fn start(self: Arc<Self>) {
        let term = self.core.term.fetch_add(1, Ordering::AcqRel) + 1;
        info!("Candiate {} start election with term {term}", self.core.me);

        let args = RequestVoteArgs {
            from: self.core.me,
            term,
            last_log: self.logs.last_log_info().await
        };
        let args = Arc::new(args);

        for peer in self.core.rpc_client.peers() {
            tokio::spawn(self.clone().poll(peer, args.clone()));
        }

        tokio::time::sleep(
            common::gen_rand_duration(common::ELECTION_TIMEOUT)
        ).await;
        if let Ok(_) = self.ev_q.put(Event::ElectionTimeout).await {
            info!("Candidate {} election timeout", self.core.me);
        }
    }

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
        assert!(voter < self.core.rpc_client.n());
        match reply.vote {
            VoteStatus::Granted => {
                let _ = self.ev_q.put(Event::GrantVote {
                    voter: reply.voter,
                }).await;
            },
            VoteStatus::Denied { term } => {
                let myterm = self.core.term.load(Ordering::Acquire);
                if myterm <= term {
                    let _ = self.ev_q.put(Event::OutdateCandidate {
                        new_term: term
                    }).await;
                }
            },
            VoteStatus::Rejected { term } => {
                let myterm = self.core.term.load(Ordering::Acquire);
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
        write!(f, "Candidate[{}, term={}]", self.core.me, self.core.term())
    }
}

impl std::fmt::Display for Poll {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Candidate[{}, term={}]", self.core.me, self.core.term())
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
