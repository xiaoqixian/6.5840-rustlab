// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc};

use labrpc::{client::ClientEnd, err::{DISCONNECTED, TIMEOUT}};
use serde::{Deserialize, Serialize};

use crate::{common, event::Event, follower::Follower, log::Logs, raft::RaftCore, role::{RoleEvQueue, Trans}, service::{AppendEntriesArgs, AppendEntriesReply, EntryStatus, RequestVoteArgs, RequestVoteReply}, OneTx};

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
            core, logs, ev_q
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
                None
            },
            Event::AppendEntries {args, reply_tx} => {
                self.append_entries(args, reply_tx).await
            },
            Event::RequestVote {args, reply_tx} => {
                self.request_vote(args, reply_tx).await
            },

            // candidate related events
            Event::GrantVote { voter } => {
                self.audit_vote(voter).await
            }
            ev => panic!("Unexpected event for a candidate: {ev}")
        }
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
        reply_tx: OneTx<AppendEntriesReply>) -> Option<Trans> {
        let myterm = self.core.term();
        
        let (reply, ret) = if args.term >= myterm {
            (
                AppendEntriesReply {
                    entry_status: EntryStatus::Retry
                },
                Some(Trans::ToFollower)
            )
        } else {
            (
                AppendEntriesReply {
                    entry_status: EntryStatus::Stale {
                        new_term: myterm
                    }
                },
                None
            )
        };

        reply_tx.send(reply).unwrap();
        ret
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
    async fn request_vote(&self, args: RequestVoteArgs, 
        reply_tx: OneTx<RequestVoteReply>) -> Option<Trans> 
    {
        let myterm = self.core.term();

        use std::cmp;
        let (vote, ret) = match myterm.cmp(&args.term) {
            cmp::Ordering::Less => {
                (VoteStatus::Rejected { term: myterm }, None)
            },
            cmp::Ordering::Equal => {
                let vote_for = self.core.vote_for.lock().await.clone();
                let vote = match vote_for {
                    Some(vote_for) if vote_for == args.id => VoteStatus::Granted,
                    _ => VoteStatus::Rejected { term: myterm }
                };
                (vote, None)
            },
            cmp::Ordering::Greater => {
                self.core.set_term(args.term);
                let up_to_date = self.logs.up_to_date(&args.last_log).await;
                let vote = if up_to_date {
                    *self.core.vote_for.lock().await = Some(args.id);
                    VoteStatus::Granted
                } else {
                    VoteStatus::Rejected {
                        term: myterm
                    }
                };
                (vote, Some(Trans::ToFollower))
            }
        };
        
        let reply = RequestVoteReply {
            voter: self.core.me,
            vote
        };
        reply_tx.send(reply).unwrap();
        ret
    }

    async fn audit_vote(&self, voter: usize) -> Option<Trans> {
        assert!(voter != self.core.me && voter < self.core.rpc_client.n(), 
            "Invalid voter id {voter}");
        let voted = self.poll.voters[voter].swap(true, Ordering::AcqRel);
        if !voted {
            let votes = self.poll.votes.fetch_add(1, Ordering::AcqRel) + 1;
            if votes >= self.poll.quorum {
                return Some(Trans::ToLeader);
            }
        }
        None
    }
}

impl Poll {
    async fn start(self: Arc<Self>) {
        let args = RequestVoteArgs {
            id: self.core.me,
            term: self.core.term(),
            last_log: self.logs.last_log_info().await
        };
        let args = Arc::new(args);

        for peer in self.core.rpc_client.peers() {
            tokio::spawn(self.clone().poll(peer, args.clone()));
        }

        tokio::time::sleep(
            common::gen_rand_duration(common::ELECTION_TIMEOUT)
        ).await;
        self.ev_q.put(Event::ElectionTimeout).await;
    }

    async fn poll(self: Arc<Self>, peer: ClientEnd, 
        args: Arc<RequestVoteArgs>) {
        const TRIES: usize = 5;
        let mut tries = 0usize;

        let ret = loop {
            let ret = peer.call::<_, RequestVoteReply>(
                crate::common::REQUEST_VOTE,
                args.as_ref()
            ).await;
            
            match ret {
                Ok(r) => break Some(r),
                Err(TIMEOUT) => {
                    if tries == TRIES {
                        break None;
                    }
                },
                Err(DISCONNECTED) => break None,
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
