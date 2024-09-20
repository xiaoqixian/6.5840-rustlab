// Date:   Thu Sep 05 09:41:37 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc}, time::Duration};

use crate::{
    candidate::{Candidate, VoteStatus}, common, event::{Event, TO_CANDIDATE}, info, leader::Leader, log::Logs, raft::RaftCore, role::{RoleEvQueue, Trans}, service::{AppendEntriesArgs, AppendEntriesReply, EntryStatus, QueryEntryArgs, QueryEntryReply, RequestVoteArgs, RequestVoteReply}, warn, OneTx
};

struct Timer {
    key: AtomicUsize,
    active: AtomicBool,
    ev_q: RoleEvQueue,
    id: usize,
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
        let heartbeat_timer = Timer::new(ev_q.clone(), core.me);
        Self {
            core,
            logs,
            ev_q,
            heartbeat_timer
        }
    }

    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        match ev {
            Event::Trans(Trans::ToFollower {..}) => 
                panic!("Follower receives a Trans::ToFollower"),
            Event::Trans(to) => return Some(to),

            Event::GetState(tx) => {
                tx.send((self.core.term(), false)).unwrap();
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

            // follower related events
            Event::HeartBeatTimeout => {
                info!("{}: HeartBeatTimeout", self);
                let _ = self.ev_q.put(TO_CANDIDATE).await;
            }

            ev => panic!("Unexpected event for a follower: {ev}")
        }
        None
    }

    pub async fn stop(&mut self) {
        self.heartbeat_timer.stop();
    }

    async fn append_entries(&self, args: AppendEntriesArgs, reply_tx: 
        OneTx<AppendEntriesReply>) 
    {
        let myterm = self.core.term();
        let entry_status = if args.term < myterm {
            EntryStatus::Stale {
                term: myterm
            }
        } else {
            if myterm < args.term {
                self.core.set_term(args.term);
            }
            // TODO: actually append entries
            self.heartbeat_timer.clone().restart();
            EntryStatus::Confirmed
        };
        let reply = AppendEntriesReply {
            from: self.core.me,
            entry_status
        };

        reply_tx.send(reply).unwrap();
    }

    async fn request_vote(&self, args: RequestVoteArgs, reply_tx: 
        OneTx<RequestVoteReply>) 
    {
        let myterm = self.core.term();

        let vote = if args.term <= myterm {
            match self.core.vote_for.lock().await.clone() {
                Some(vote_for) if vote_for == args.from => {
                    VoteStatus::Granted
                },
                _ => VoteStatus::Rejected { term: myterm }
            }
        } else {
            self.core.set_term(args.term);
            if self.logs.up_to_date(&args.last_log).await {
                *self.core.vote_for.lock().await = Some(args.from);
                self.heartbeat_timer.clone().restart();
                VoteStatus::Granted
            } else {
                VoteStatus::Rejected { term: myterm }
            }
        };

        let reply = RequestVoteReply {
            voter: self.core.me,
            vote
        };
        reply_tx.send(reply).unwrap();
    }

    async fn query_entry(&self, args: QueryEntryArgs, reply_tx: OneTx<QueryEntryReply>) {
        let reply = if self.logs.log_exist(&args.log_info).await {
            QueryEntryReply::Exist
        } else { QueryEntryReply::NotExist };
        reply_tx.send(reply).unwrap();
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
    fn new(ev_q: RoleEvQueue, id: usize) -> Arc<Self> {
        let inst = Self {
            key: AtomicUsize::new(0),
            active: AtomicBool::new(true),
            ev_q,
            id
        };
        let inst = Arc::new(inst);
        let d = Self::gen_d();
        tokio::spawn(inst.clone().round(d, 0));
        inst
    }
    
    async fn round(self: Arc<Self>, d: Duration, round_key: usize) {
        info!("Follower {} heartbeat timeout after {}ms", self.id, d.as_millis());
        tokio::time::sleep(d).await;
        if !self.stopped() && round_key == self.key.load(Ordering::Acquire) {
            info!("Follower {} heartbeat timeout", self.id);
            if let Err(_) = self.ev_q.put(Event::HeartBeatTimeout).await {
                warn!("Timer put HeartBeatTimeout failed");
            }
        }
    }

    fn restart(self: Arc<Self>) {
        let d = Self::gen_d();
        if !self.stopped() {
            let key = self.key.fetch_add(1, Ordering::AcqRel) + 1;
            tokio::spawn(self.clone().round(d, key));
        }
    }

    fn stop(&self) {
        self.active.store(false, Ordering::Release);
    }

    fn stopped(&self) -> bool {
        !self.active.load(Ordering::Acquire)
    }

    fn gen_d() -> Duration {
        common::gen_rand_duration(common::HEARTBEAT_TIMEOUT)
    }
}

impl std::fmt::Display for Follower {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Follower[{}, term={}]", self.core.me, self.core.term())
    }
}
