// Date:   Fri Sep 06 16:10:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{candidate::VoteStatus, event::Event, log::{LogEntry, LogInfo}, raft::RaftCore};
use serde::{Serialize, Deserialize};

pub type RequestVoteRes = Result<RequestVoteReply, ()>;
pub type AppendEntriesRes = Result<AppendEntriesReply, ()>;

/// Can be 3 items:
/// - A heart beat
/// - A list of entries
/// - A snapshot
#[derive(Serialize, Deserialize)]
pub enum AppendEntriesType {
    HeartBeat,
    Entries {
        prev: LogInfo,
        entries: Vec<LogEntry>
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EntryStatus {
    Retry,
    Confirmed,
    Stale {
        term: usize
    }
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub from: usize,
    pub term: usize,
    pub entry_type: AppendEntriesType
}
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub from: usize,
    pub entry_status: EntryStatus
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub from: usize,
    pub term: usize,
    // the index and term of the last log
    pub last_log: LogInfo
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub voter: usize,
    // term represents the term of the request that 
    // this reply response to.
    // Without term, the receiver may confuse earlier term 
    // responses with current term responses, and cause vote 
    // inconsistent.
    // pub term: usize,
    pub vote: VoteStatus
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QueryEntryArgs {
    pub log_info: LogInfo
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryEntryReply {
    Exist,
    NotExist,
}
pub type QueryEntryRes = Result<QueryEntryReply, ()>;

pub struct RpcService {
    core: RaftCore
}

impl RpcService {
    pub fn new(core: RaftCore) -> Self {
        Self { core }
    }
}

// macro_rules! build_rpc {
//     ($name: ident, $arg_ty: ty, $res_ty: ty, $ev: ident) => {
//         pub async fn $name(&self, args: $arg_ty) -> $res_ty {
//             if self.core.dead() {
//                 return Err(());
//             }
//             let (tx, rx) = tokio::sync::oneshot::channel();
//             let ev = Event::$ev {
//                 args,
//                 reply_tx: tx
//             };
//             self.core.ev_q.just_put(ev).await;
//             Ok(rx.await.unwrap())
//         }
//     }
// }

#[labrpc_macros::rpc]
impl RpcService {
    /// The AppendEntries event may not be processed.
    /// For instance, the Raft node may already be dead.
    pub async fn append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesRes {
        if self.core.dead() {
            return Err(());
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::AppendEntries {
            args,
            reply_tx: tx
        };
        self.core.ev_q.just_put(ev).await;
        Ok(rx.await.unwrap())
    }

    /// Request a vote from this raft node.
    pub async fn request_vote(&self, args: RequestVoteArgs) -> RequestVoteRes {
        if self.core.dead() {
            return Err(());
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::RequestVote {
            args,
            reply_tx: tx
        };
        self.core.ev_q.just_put(ev).await;
        Ok(rx.await.unwrap())
    }

    pub async fn query_entry(&self, args: QueryEntryArgs) -> QueryEntryRes {
        if self.core.dead() {
            return Err(());
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::QueryEntry {
            args,
            reply_tx: tx
        };
        self.core.ev_q.just_put(ev).await;
        Ok(rx.await.unwrap())
    }
}
