// Date:   Thu Sep 05 09:41:37 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use crate::{candidate::Candidate, common::{self, HEARTBEAT_TIMEOUT}, event::{EvQueue, Event}, leader::Leader, log::Logs, raft::{Raft, RaftCore}, role::{RoleEvQueue, Trans}};

pub struct Follower {
    pub core: RaftCore,
    pub logs: Logs,
    pub ev_q: RoleEvQueue
}

impl Follower {
    pub fn new(core: RaftCore, logs: Logs) -> Self {
        let ev_q = RoleEvQueue::new(core.ev_q.clone(), 0);
        tokio::spawn(Self::start_timer(ev_q.clone()));
        Self {
            core,
            logs,
            ev_q
        }
    }

    async fn start_timer(ev_q: RoleEvQueue) {
        let d = common::gen_rand_duration(HEARTBEAT_TIMEOUT);
        tokio::time::sleep(d).await;
        // heartbeat timer does not care if the event is 
        // successfully put into queue.
        let _ = ev_q.put(Event::HeartBeatTimeout).await;
    }

    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        None
    }

    pub async fn stop(&mut self) {}
}

impl From<Candidate> for Follower {
    fn from(cd: Candidate) -> Self {
        Self {
            core: cd.core,
            logs: cd.logs,
            ev_q: cd.ev_q.transfer()
        }
    }
}

impl From<Leader> for Follower {
    fn from(ld: Leader) -> Self {
        Self {
            core: ld.core,
            logs: ld.logs,
            ev_q: ld.ev_q.transfer()
        }
    }
}
