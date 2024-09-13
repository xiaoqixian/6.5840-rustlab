// Date:   Thu Sep 05 09:41:37 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc}, time::Duration};

use crate::{candidate::Candidate, common::{self, HEARTBEAT_TIMEOUT}, event::{EvQueue, Event}, leader::Leader, log::Logs, raft::{Raft, RaftCore}, role::{RoleEvQueue, Trans}, service::{AppendEntriesArgs, AppendEntriesReply}, OneTx};

struct Timer {
    key: AtomicUsize,
    active: AtomicBool,
    ev_q: RoleEvQueue
}

pub struct Follower {
    pub core: RaftCore,
    pub logs: Logs,
    pub ev_q: RoleEvQueue,
    heartbeat_timer: Arc<Timer>
}

impl Follower {
    pub fn new(core: RaftCore, logs: Logs) -> Self {
        let ev_q = RoleEvQueue::new(core.ev_q.clone(), 0);
        Self::new_with_ev_q(core, logs, ev_q)
    }

    pub fn new_with_ev_q(core: RaftCore, logs: Logs, ev_q: RoleEvQueue) -> Self {
        let heartbeat_timer = {
            let d = common::gen_rand_duration(HEARTBEAT_TIMEOUT);
            Timer::new(ev_q.clone(), d)
        };

        Self {
            core,
            logs,
            ev_q,
            heartbeat_timer
        }
    }

    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        None
    }

    pub async fn stop(&mut self) {}

    async fn append_entries(&self, args: AppendEntriesArgs, 
        reply_tx: OneTx<AppendEntriesReply>) 
    {
        
    }
}

impl From<Candidate> for Follower {
    fn from(cd: Candidate) -> Self {
        Self::new_with_ev_q(cd.core, cd.logs, cd.ev_q.transfer())
    }
}

impl From<Leader> for Follower {
    fn from(ld: Leader) -> Self {
        Self::new_with_ev_q(ld.core, ld.logs, ld.ev_q.transfer())
    }
}

impl Timer {
    fn new(ev_q: RoleEvQueue, d: Duration) -> Arc<Self> {
        let inst = Self {
            key: AtomicUsize::new(0),
            active: AtomicBool::new(true),
            ev_q
        };
        let inst = Arc::new(inst);
        tokio::spawn(inst.clone().round(d, 0));
        inst
    }
    
    async fn round(self: Arc<Self>, d: Duration, round_key: usize) {
        tokio::time::sleep(d).await;
        if !self.stopped() && round_key == self.key.load(Ordering::Acquire) {
            let _ = self.ev_q.put(Event::HeartBeatTimeout).await;
        }
    }

    fn restart(self: Arc<Self>, d: Duration) {
        if !self.stopped() {
            let key = self.key.fetch_add(1, Ordering::AcqRel) + 1;
            tokio::spawn(self.clone().round(d, key));
        }
    }

    fn stop(&self) {
        self.active.store(false, Ordering::Release);
    }

    fn stopped(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
}

