// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc};

use labrpc::{client::ClientEnd, err::{DISCONNECTED, TIMEOUT}};
use serde::{Deserialize, Serialize};

use crate::{common, event::Event, follower::Follower, log::Logs, raft::RaftCore, role::{RoleEvQueue, Trans}, service::{RequestVoteArgs, RequestVoteReply}};

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
        None
    }

    pub async fn stop(&self) {
        self.active.store(false, Ordering::Release);
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
                self.ev_q.put(Event::GrantVote {
                    term: reply.term,
                    voter: reply.voter,
                }).await;
            },
            VoteStatus::Denied { term } => {
                let myterm = self.core.term.load(Ordering::Acquire);
                if myterm <= term {
                    self.ev_q.put(Event::OutdateCandidate {
                        new_term: term
                    }).await;
                }
            },
            VoteStatus::Rejected { term } => {
                let myterm = self.core.term.load(Ordering::Acquire);
                if myterm < term {
                    self.ev_q.put(Event::OutdateCandidate {
                        new_term: term
                    }).await;
                }
            }
        }
    }
}
