// Date:   Thu Sep 05 09:41:37 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use crate::{event::{EvQueue, Event}, raft::{Raft, RaftCore}, utils::{self, HEARTBEAT_TIMEOUT}, Outcome};

pub struct Follower {
    pub(crate) core: Arc<RaftCore>,
}

impl Follower {
    pub fn new(core: Arc<RaftCore>) -> Self {
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

    pub async fn process(&mut self, ev: Event) -> Option<Outcome> {
        None
    }
}
