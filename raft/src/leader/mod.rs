// Date:   Mon Sep 30 21:44:44 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, Ordering}, Arc};
use serde::{Serialize, ser::SerializeStruct};

use crate::{
    candidate::VoteStatus,
    debug,
    event::{Event, TO_FOLLOWER},
    info,
    raft::RaftCore,
    role::{RoleCore, RoleEvQueue, Trans},
    service::{
        AppendEntriesArgs, AppendEntriesReply, EntryStatus, QueryEntryArgs, QueryEntryReply,
        RequestVoteArgs, RequestVoteReply,
    },
    utils::Peer,
    warn, ApplyMsg,
};

mod replicator;
mod counter;
mod ldlogs;
use counter::ReplCounter;
use ldlogs::{LdLogs, ReplLogs};
use replicator::Replicator;

pub struct Leader {
    core: RaftCore,
    logs: LdLogs,
    ev_q: RoleEvQueue,
    active: Arc<AtomicBool>,
    repl_counter: ReplCounter,
}

impl Leader {
    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        match ev {
            Event::Trans(Trans::ToLeader) => 
                panic!("Leader receives a Trans::ToLeader"),
            Event::Trans(to) => return Some(to),
            
            Event::GetState(tx) => {
                tx.send((self.core.term, true)).unwrap();
            },

            Event::StartCmd { cmd, reply_tx } => {
                self.start_cmd(cmd, reply_tx).await;
            }

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
            },

            Event::UpdateCommit(lci) => {
                info!("{self}: update commit to {lci}");
                let apply_cmds = self.logs.update_commit(lci);
                let success = self.persist_state();
                if success {
                    for (index, command) in apply_cmds.into_iter() {
                        // it's OK if the apply operation failed
                        if let Err(_) = self.core.apply_ch.send(
                            ApplyMsg::Command { index, command }
                        ) {
                            break
                        }
                    }
                }
            },

            Event::StaleLeader {new_term} => {
                self.core.term = new_term;
                let _ = self.ev_q.put(TO_FOLLOWER);
                self.persist_state();
            }

            ev => panic!("Unexpected event for a leader: {ev}")
        }
        None
    }

    pub fn stop(self) -> RoleCore {
        self.active.store(false, Ordering::Relaxed);
        self.into()
    }

    async fn start_cmd(&self, cmd: Vec<u8>, reply_tx: tokio::sync::oneshot::Sender<Option<(usize, usize)>>) {
        let term = self.core.term;
        let (idx, cmd_idx) = self.logs.push_cmd(term, cmd);
        debug!("{self}: start a command, index = {idx}, command index = {cmd_idx}");
        if !self.persist_state() {
            return
        }
        self.repl_counter.watch_idx(idx);
        if let Err(_) = reply_tx.send(Some((cmd_idx, term))) {
            warn!("start_cmd() reply failed");
        }
    }

    async fn append_entries(&mut self, args: AppendEntriesArgs, reply_tx: 
        tokio::sync::oneshot::Sender<AppendEntriesReply>) 
    {
        let myterm = self.core.term;

        let entry_status = if args.term < myterm {
            EntryStatus::Stale {
                term: myterm
            }
        } else if args.term == myterm {
            panic!("Leader {} and {} has the same term {}", 
                self.core.me, args.from, myterm);
        } else {
            debug!("{self}: {} is a leader with greater term, become a follower.", args.from);
            self.core.term = args.term;
            self.persist_state();
            let _ = self.ev_q.put(TO_FOLLOWER);
            EntryStatus::Hold
        };
        let reply = AppendEntriesReply {
            from: self.core.me,
            entry_status
        };
        reply_tx.send(reply).unwrap();
    }

    async fn request_vote(&mut self, args: RequestVoteArgs, reply_tx: 
        tokio::sync::oneshot::Sender<RequestVoteReply>)
    {
        let myterm = self.core.term;
        
        let vote = if args.term <= myterm {
            debug!("{self}: denied vote from ({}, {}), I'm the real leader.", args.from, args.term);
            VoteStatus::Denied { term: myterm }
        } else {
            debug!("{self}: candidate {} has a greater term.", args.from);
            self.core.term = args.term;
            let _ = self.ev_q.put(TO_FOLLOWER);
            
            let status = if self.logs.up_to_date(&args.last_log) {
                self.core.vote_for = Some(args.from);
                debug!("{self}: grant vote to {}", args.from);
                VoteStatus::Granted
            } else {
                // even myterm here is outdated, it makes no difference
                // to the remote candidate, the candidate will not quit 
                // election as long as the Reject.term is not greater 
                // than the candidate's.
                debug!("{self}: reject vote from {} for not up_to_date logs.", args.from);
                VoteStatus::Rejected {term: myterm}
            };
            if !self.persist_state() {
                warn!("{self}: the persister is down, exit!");
                return
            }
            status
        };

        let reply = RequestVoteReply {
            voter: self.core.me,
            vote
        };
        reply_tx.send(reply).unwrap();
    }

    async fn query_entry(&self, args: QueryEntryArgs, reply_tx: tokio::sync::oneshot::Sender<QueryEntryReply>) {
        let reply = if self.logs.log_exist(&args.log_info) {
            QueryEntryReply::Exist
        } else { QueryEntryReply::NotExist };
        reply_tx.send(reply).unwrap();
    }

    fn persist_state(&self) -> bool {
        let state = bincode::serialize(self).unwrap();
        self.core.persister.save(Some(state), None, false)
    }
}

impl From<RoleCore> for Leader {
    fn from(role_core: RoleCore) -> Self {
        let RoleCore {
            raft_core: core,
            mut logs,
            ev_q
        } = role_core;

        debug!("{core} become a leader");

        // when a node become a leader, it pushes a noop log to its logs,
        // this will be the first log entry in its reign.
        let noop_idx = logs.push_noop(core.term);
        let lci = logs.lci();

        #[cfg(feature = "no_debug")]
        let ld_logs = LdLogs::from(logs);
        #[cfg(not(feature = "no_debug"))]
        let ld_logs = LdLogs::from((core.me, logs));

        // spawn heartbeat senders
        let active = Arc::new(AtomicBool::new(true));
        let repl_counter = ReplCounter::new(core.me, core.rpc_client.n(), 
            lci, ev_q.clone(), active.clone());

        for idx in (lci+1)..=noop_idx {
            repl_counter.watch_idx(idx);
        }
        debug!("Leader[{}]: watch logs in range {:?}", core.me, (lci+1)..=noop_idx);

        for peer in core.rpc_client.peers() {
            #[cfg(feature = "no_debug")]
            let repl_logs = ReplLogs::from(&ld_logs);
            #[cfg(not(feature = "no_debug"))]
            let repl_logs = ReplLogs::from((peer.to(), &ld_logs));
            tokio::spawn(Replicator::new(
                core.me,
                core.term,
                repl_logs,
                Peer::new(peer, active.clone()),
                ev_q.clone(),
                // next_index init value does not matter, it will be 
                // re-assigned soon.
                0,
                repl_counter.clone()
            ).start());
        }

        Self {
            core,
            logs: ld_logs,
            ev_q,
            active,
            repl_counter
        }
    }
}

impl Into<RoleCore> for Leader {
    fn into(self) -> RoleCore {
        RoleCore {
            raft_core: self.core,
            logs: self.logs.into(),
            ev_q: self.ev_q.transfer()
        }
    }
}

impl std::fmt::Display for Leader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Leader[{}, term={}, last_log={}]", self.core.me, self.core.term,  self.logs.last_log_info())
    }
}

impl Serialize for Leader {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
        let mut s = serializer.serialize_struct("RaftState", 2)?;
        s.serialize_field("raft_info", &self.core)?;
        s.serialize_field("logs_info", &self.logs)?;
        s.end()
    }
}