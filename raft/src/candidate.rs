// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, Ordering}, Arc};

use serde::{Deserialize, Serialize};

use crate::{common, event::Event, follower::Follower, log::Logs, raft::RaftCore, role::Trans, service::{RequestVoteArgs, RequestVoteReply}};

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
    pub logs: Logs,
    active: Arc<AtomicBool>,
}

impl From<Follower> for Candidate {
    fn from(flw: Follower) -> Self {
        Self {
            core: flw.core,
            logs: flw.logs,
            active: Default::default()
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

    async fn start_election(self) {
        let args = RequestVoteArgs {
            id: self.core.me,
            term: self.core.term(),
            last_log: self.logs.last_log_info().await
        };

        // my own vote is not included
        let mut votes = 0usize;
        let (mut poll_ch, n) = self.core.rpc_client.broadcast::<_, RequestVoteReply>(
            common::REQUEST_VOTE, args
        ).await.unwrap();
        let quorum = n / 2;

        while let Some(resp) = poll_ch.recv().await {
            
        }
    }
}
