// Date:   Thu Sep 05 11:00:39 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, Ordering}, Arc};

use crate::{candidate::{Candidate, VoteStatus}, common::{self, RpcRetry, RPC_FAIL_RETRY}, event::Event, info, log::{LogInfo, LogPack, Logs}, raft::RaftCore, role::{RoleEvQueue, Trans}, service::{AppendEntriesArgs, AppendEntriesReply, AppendEntriesRes, AppendEntriesType, EntryStatus, QueryEntryArgs, QueryEntryReply, QueryEntryRes, RequestVoteArgs, RequestVoteReply}, utils::Peer, OneTx};

pub struct Leader {
    pub core: RaftCore,
    pub logs: Logs,
    pub ev_q: RoleEvQueue,
    active: Arc<AtomicBool>
}

struct Replicator {
    core: RaftCore,
    logs: Logs,
    active: Arc<AtomicBool>,
    peer: Peer,
    ev_q: RoleEvQueue,
    next_index: usize
}

impl Replicator {
    // find the matched next_index with the client
    // use binary search
    async fn match_index(&mut self) {
        'search: while self.active.load(Ordering::Acquire) {
            let (mut l, mut r) = self.logs.lii_lli().await;
            'round: while self.active.load(Ordering::Acquire) && l <= r {
                let mid = l + (r - l)/2;
                let mid_term = match self.logs.index_term(mid).await {
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
                        l = mid + 1;
                    },
                    QueryEntryReply::NotExist => {
                        r = mid - 1;
                    },
                    QueryEntryReply::Stale {new_term} => {
                        let _ = self.ev_q.put(Event::Trans(Trans::ToFollower {
                            new_term: Some(new_term)
                        })).await;
                        return;
                    }
                }
            }

            self.next_index = r;
            break 'search;
        }
    }

    async fn start(mut self) {
        self.match_index().await;
        info!("{self}: find index {}", self.next_index);

        let mut hb_ticker = tokio::time::interval(common::HEARTBEAT_INTERVAL);

        'repl: while self.active.load(Ordering::Acquire) {
            hb_ticker.tick().await;

            let (entry_type, next_next_index) = match 
                self.logs.repl_get(self.next_index).await {
                None => (AppendEntriesType::HeartBeat, self.next_index),
                Some((prev, entries)) => {
                    let next_next_index = entries.last().unwrap().index;
                    (
                        AppendEntriesType::Entries {
                            prev,
                            entries
                        },
                        next_next_index
                    )
                }
            };

            let args = AppendEntriesArgs {
                id: self.core.me,
                term: self.core.term(),
                entry_type
            };

            // keep_call may take very long
            let reply = self.peer.call::<_, AppendEntriesReply, ()>(
                common::APPEND_ENTRIES,
                &args
            ).await.unwrap();
            
            match reply.entry_status {
                EntryStatus::Stale { term } => {
                    if term > self.core.term() {
                        let _ = self.ev_q.put(Event::Trans(Trans::ToFollower {
                            new_term: Some(term)
                        })).await;
                        break 'repl;
                    }
                },
                EntryStatus::Confirmed => {
                    self.next_index = next_next_index;
                },
                EntryStatus::Retry => {}
            }
        }
    }
}

impl Leader {
    fn new(core: RaftCore, logs: Logs, ev_q: RoleEvQueue) -> Self {
        info!("{core} become a leader");
        // spawn heartbeat senders
        let active = Arc::new(AtomicBool::new(true));
        for peer in core.rpc_client.peers() {
            tokio::spawn(Replicator {
                core: core.clone(),
                logs: logs.clone(),
                active: active.clone(),
                peer: Peer::new(peer, active.clone()),
                ev_q: ev_q.clone(),
                // next_index init value does not matter, it will be 
                // re-assigned soon.
                next_index: 0
            }.start());
        }
        Self {
            core,
            logs,
            ev_q,
            active
        }
    }

    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        match ev {
            Event::Trans(Trans::ToLeader) => 
                panic!("Leader receives a Trans::ToLeader"),
            Event::Trans(to) => return Some(to),

            Event::AppendEntries {args, reply_tx} => {
                info!("{self}: AppendEntries from {}, term={}", args.id, args.term);
                self.append_entries(args, reply_tx).await;
            },
            Event::RequestVote {args, reply_tx} => {
                info!("{self}: RequestVote from {}, term={}", args.id, args.term);
                self.request_vote(args, reply_tx).await;
            },
            
            Event::GetState(tx) => {
                tx.send((self.core.term(), true)).unwrap();
            }

            ev => panic!("Unexpected event for a leader: {ev}")
        }
        None
    }

    pub async fn stop(&mut self) {
        self.active.store(false, Ordering::Release);
    }

    async fn append_entries(&self, args: AppendEntriesArgs, reply_tx: 
        OneTx<AppendEntriesReply>) 
    {
        let myterm = self.core.term();

        let entry_status = if args.term < myterm {
            EntryStatus::Stale {
                term: myterm
            }
        } else if args.term == myterm {
            panic!("Leader {} and {} has the same term {}", 
                self.core.me, args.id, myterm);
        } else {
            let _ = self.ev_q.put(Event::Trans(Trans::ToFollower {
                new_term: Some(args.term)
            })).await;
            EntryStatus::Retry
        };
        let reply = AppendEntriesReply {
            id: self.core.me,
            entry_status
        };
        reply_tx.send(reply).unwrap();
    }

    async fn request_vote(&self, args: RequestVoteArgs, reply_tx: 
        OneTx<RequestVoteReply>)
    {
        let myterm = self.core.term();
        
        let vote = if args.term <= myterm {
            VoteStatus::Denied { term: myterm }
        } else {
            let _ = self.ev_q.put(Event::Trans(Trans::ToFollower {
                new_term: Some(args.term)
            })).await;
            
            if self.logs.up_to_date(&args.last_log).await {
                *self.core.vote_for.lock().await = Some(args.id);
                VoteStatus::Granted
            } else {
                // even myterm here is outdated, it makes no difference
                // to the remote candidate, the candidate will not quit 
                // election as long as the Reject.term is not greater 
                // than the candidate's.
                VoteStatus::Rejected {term: myterm}
            }
        };

        let reply = RequestVoteReply {
            voter: self.core.me,
            vote
        };
        reply_tx.send(reply).unwrap();
    }
}

impl From<Candidate> for Leader {
    fn from(cd: Candidate) -> Self {
        Self::new(cd.core, cd.logs, cd.ev_q.transfer())
    }
}

impl std::fmt::Display for Leader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Leader[{}, term={}]", self.core.me, self.core.term())
    }
}

impl std::fmt::Display for Replicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Repl[{}->{}]", self.core.me, self.peer.to())
    }
}
