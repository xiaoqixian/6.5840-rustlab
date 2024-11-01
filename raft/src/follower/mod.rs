// Date:   Thu Oct 03 19:50:34 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::time::Duration;

use crate::{
    candidate::VoteStatus, common, event::{Event, TO_CANDIDATE}, info, logs::Logs, raft::RaftCore, role::{RoleCore, RoleEvQueue, Trans}, service::{AppendEntriesArgs, AppendEntriesReply, AppendEntriesType, EntryStatus, QueryEntryArgs, QueryEntryReply, RequestVoteArgs, RequestVoteReply}, OneTx
};

mod timer;
use serde::Serialize;
use timer::Timer;

#[derive(Serialize)]
pub struct Follower {
    core: RaftCore,
    logs: Logs,
    #[serde(skip)]
    ev_q: RoleEvQueue,
    #[serde(skip)]
    hb_timer: Timer
}

impl Follower {
    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        match ev {
            Event::Trans(Trans::ToFollower {..}) => 
                panic!("Follower receives a Trans::ToFollower"),
            Event::Trans(to) => return Some(to),

            Event::GetState(tx) => {
                tx.send((self.core.term, false)).unwrap();
            },

            Event::StartCmd { reply_tx, .. } => {
                let _ = reply_tx.send(None);
            }

            Event::AppendEntries {args, reply_tx} => {
                info!("{self}: {}", args);
                self.append_entries(args, reply_tx).await;
            },
            Event::RequestVote {args, reply_tx} => {
                info!("{self}: {}", args);
                self.request_vote(args, reply_tx).await;
            },
            Event::QueryEntry {args, reply_tx} => {
                info!("{self}: QueryEntry, log_info = {}", args.log_info);
                self.query_entry(args, reply_tx).await;
            }

            // follower related events
            Event::HeartBeatTimeout => {
                info!("{}: HeartBeatTimeout", self);
                let _ = self.ev_q.put(TO_CANDIDATE);
            }

            ev => panic!("Unexpected event for a follower: {ev}")
        }
        None
    }

    pub fn stop(self) -> RoleCore {
        self.into()
    }

    async fn append_entries(&mut self, args: AppendEntriesArgs, reply_tx: 
        OneTx<AppendEntriesReply>) 
    {
        let myterm = self.core.term;
        let entry_status = if args.term < myterm {
            EntryStatus::Stale {
                term: myterm
            }
        } else {
            if myterm < args.term {
                self.core.term = args.term;
            }
            self.hb_timer.reset();
            let entry_status = match args.entry_type {
                AppendEntriesType::HeartBeat => EntryStatus::Confirmed,
                AppendEntriesType::Entries {prev, entries} => {
                    if let Err(entries) = self.logs.try_merge(&prev, entries) {
                        panic!("{:?} should not be rejected", 
                            AppendEntriesType::Entries {prev, entries});
                    }
                    EntryStatus::Confirmed
                }
            };
            self.logs.update_commit(args.lci);
            self.persist_state().await;
            entry_status
        };
        let reply = AppendEntriesReply {
            from: self.core.me,
            entry_status
        };

        reply_tx.send(reply).unwrap();
    }

    async fn request_vote(&mut self, args: RequestVoteArgs, reply_tx: 
        OneTx<RequestVoteReply>) 
    {
        let myterm = self.core.term;

        let vote = if args.term <= myterm {
            match &self.core.vote_for {
                Some(vote_for) if *vote_for == args.from => {
                    VoteStatus::Granted
                },
                _ => VoteStatus::Rejected { term: myterm }
            }
        } else {
            self.core.term = args.term;
            let vote = if self.logs.up_to_date(&args.last_log) {
                self.core.vote_for = Some(args.from);
                self.hb_timer.reset();
                VoteStatus::Granted
            } else {
                VoteStatus::Rejected { term: myterm }
            };
            self.persist_state().await;
            vote
        };

        let reply = RequestVoteReply {
            voter: self.core.me,
            vote
        };
        reply_tx.send(reply).unwrap();
    }

    async fn query_entry(&self, args: QueryEntryArgs, reply_tx: OneTx<QueryEntryReply>) {
        let reply = if self.logs.log_exist(&args.log_info) {
            QueryEntryReply::Exist
        } else { QueryEntryReply::NotExist };
        reply_tx.send(reply).unwrap();
    }

    async fn persist_state(&self) {
        let state = bincode::serialize(self).unwrap();
        self.core.persister.save(Some(state), None);
    }
}

impl From<RoleCore> for Follower {
    fn from(core: RoleCore) -> Self {
        let hb_timer = {
            let hb_timeout = {
                let ev_q = core.ev_q.clone();
                move || {
                    let _ = ev_q.put(Event::HeartBeatTimeout);
                }
            };

            let hb_gen = || {
                use rand::Rng;
                let ms = rand::thread_rng().gen_range(common::HEARTBEAT_TIMEOUT);
                Duration::from_millis(ms)
            };
            Timer::new(hb_timeout, hb_gen)
        };
        Self {
            core: core.raft_core,
            logs: core.logs,
            ev_q: core.ev_q,
            hb_timer
        }
    }
}

impl Into<RoleCore> for Follower {
    fn into(self) -> RoleCore {
        RoleCore {
            raft_core: self.core,
            logs: self.logs,
            ev_q: self.ev_q.transfer()
        }
    }
}

impl std::fmt::Display for Follower {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Follower[{}, term={}]", self.core.me, self.core.term)
    }
}
