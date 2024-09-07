// Date:   Thu Sep 05 09:41:37 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{candidate::Candidate, event::{EvQueue, Event}, leader::Leader, raft::{Raft, RaftCore}, role::Trans, utils::{self, HEARTBEAT_TIMEOUT}};

pub struct Follower {
    pub core: RaftCore,
}

impl Follower {
    pub fn new(core: RaftCore) -> Self {
        tokio::spawn(Self::start_timer(core.ev_q.clone()));
        Self {
            core,
        }
    }

    async fn start_timer(ev_q: EvQueue) {
        let d = utils::gen_rand_duration(HEARTBEAT_TIMEOUT);
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
        Self { core: cd.core }
    }
}

impl From<Leader> for Follower {
    fn from(ld: Leader) -> Self {
        Self { core: ld.core }
    }
}
