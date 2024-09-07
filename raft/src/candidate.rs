// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, Ordering}, Arc};

use serde::{Deserialize, Serialize};

use crate::{event::Event, follower::Follower, raft::RaftCore, role::Outcome, service::RequestVoteArgs};

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
    // Denial is given by a new leader, it tells 
    // the candidate to stop its election and become 
    // a follower ASAP.
    Denial {
        term: usize
    }
}

#[derive(Clone)]
pub struct Candidate {
    pub core: RaftCore,
    active: Arc<AtomicBool>
}

impl From<Follower> for Candidate {
    fn from(flw: Follower) -> Self {
        Self {
            core: flw.core,
            active: Default::default()
        }
    }
}

impl Candidate {
    pub fn new(core: RaftCore) -> Self {
        Self { 
            core,
            active: Default::default()
        }
    }

    pub async fn process(&mut self, ev: Event) -> Option<Outcome> {
        None
    }

    pub async fn stop(&self) {
        self.active.store(false, Ordering::Release);
    }

    async fn start_election(self) {
        let args = RequestVoteArgs {
            id: self.core.me,
            term: self.core.term(),
        }
    }
}
