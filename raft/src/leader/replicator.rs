// Date:   Mon Sep 30 21:45:51 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{common::{self, RPC_FAIL_RETRY}, event::Event, info, leader::ldlogs::ReplQueryRes, logs::LogInfo, role::RoleEvQueue, service::{AppendEntriesArgs, AppendEntriesReply, AppendEntriesType, EntryStatus, QueryEntryArgs, QueryEntryReply}, utils::Peer};

use super::{counter::ReplCounter, ldlogs::ReplLogs};

pub struct Replicator {
    me: usize,
    term: usize,
    logs: ReplLogs,
    peer: Peer,
    peer_id: usize,
    ev_q: RoleEvQueue,
    next_index: usize,
    repl_counter: ReplCounter
}

impl Replicator {
    pub fn new(me: usize, term: usize, logs: ReplLogs,
        peer: Peer, ev_q: RoleEvQueue, next_index: usize, 
        repl_counter: ReplCounter) -> Self 
    {
        Self {
            me,
            term,
            logs,
            peer_id: peer.to(),
            peer,
            ev_q,
            next_index,
            repl_counter
        }
    }

    // find the matched next_index with the client
    // use binary search
    async fn match_index(&self) -> Result<usize, ()> {
        let target = 'search: loop {
            let mut target = 0usize;
            let (mut l, mut r) = {
                let rg = self.logs.logs_range()?;
                (*rg.start(), *rg.end())
            };
            info!("{self}: match_index search between [{l}, {r}]");

            'round: while l <= r {
                let mid = l + (r - l)/2;
                let mid_term = match self.logs.index_term(mid)? {
                    // A new snapshot is taken, so the term cannot be 
                    // indexed.
                    None => continue 'search,
                    Some(term) => term
                };

                let args = QueryEntryArgs {
                    log_info: LogInfo::new(mid, mid_term)
                };
                
                let reply = match self.peer.try_call::<_, QueryEntryReply, ()>(
                    common::QUERY_ENTRY,
                    &args,
                    RPC_FAIL_RETRY
                ).await {
                    None => {
                        tokio::time::sleep(common::NET_FAIL_WAIT).await;
                        continue 'round;
                    },
                    Some(r) => r
                };

                match reply {
                    QueryEntryReply::Exist => {
                        target = mid;
                        l = mid + 1;
                    },
                    QueryEntryReply::NotExist => {
                        r = mid - 1;
                    },
                }
            }

            break 'search target;
        };
        Ok(target)
    }

    pub async fn start(mut self) -> Result<(), ()> {
        self.next_index = self.match_index().await? + 1;
        info!("{self}: find index {}", self.next_index);

        let mut hb_ticker = tokio::time::interval(common::HEARTBEAT_INTERVAL);

        'repl: loop {
            hb_ticker.tick().await;

            let ReplQueryRes { lci, entry_type } = 
                self.logs.repl_get(self.next_index)?;

            let next_next_index = match &entry_type {
                AppendEntriesType::HeartBeat => self.next_index,
                AppendEntriesType::Entries {entries,..} => 
                    entries.last().unwrap().index + 1
            };

            let args = AppendEntriesArgs {
                from: self.me,
                term: self.term,
                lci,
                entry_type
            };

            // this may take very long time,
            // call only returns None when the leader is shut down, 
            // in which case we let the replicator break the loop and exit.
            let reply = match self.peer.call::<_, AppendEntriesReply, ()>(
                common::APPEND_ENTRIES,
                &args
            ).await {
                None => break 'repl,
                Some(r) => r
            };
            
            match reply.entry_status {
                EntryStatus::Stale { term } => {
                    if term > self.term {
                        let _ = self.ev_q.put(Event::StaleLeader {
                            new_term: term
                        });
                        break 'repl;
                    }
                },
                EntryStatus::Confirmed => {
                    if self.next_index < next_next_index {
                        self.repl_counter.confirm(self.peer_id, next_next_index-1);
                        info!("{self}: update next_index {} -> {next_next_index}", self.next_index);
                        self.next_index = next_next_index;
                    }
                },
                EntryStatus::Mismatched => {
                    self.next_index = self.match_index().await? + 1;
                },
                EntryStatus::Hold => {}
            }
        }
        Ok(())
    }
}

impl std::fmt::Display for Replicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Repl[{}->{}]", self.me, self.peer.to())
    }
}
