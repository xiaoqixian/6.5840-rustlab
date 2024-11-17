// Date:   Fri Sep 06 16:10:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{
    candidate::VoteStatus,
    event::Event,
    logs::{LogEntry, LogInfo},
    raft::RaftHandle,
};
use serde::{Deserialize, Serialize};

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
        entries: Vec<LogEntry>,
    },
    Snapshot {
        last_log_idx: usize,
        last_log_term: usize,
        snapshot_lii: usize,
        snapshot: Vec<u8>
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EntryStatus {
    // tell replicator to hold for a while,
    // then resend the entries.
    Hold,
    Confirmed,
    // the prev log cannot match any log
    Mismatched,
    Stale { term: usize },
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub from: usize,
    pub term: usize,
    // last committed index
    pub lci: usize,
    pub entry_type: AppendEntriesType,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub from: usize,
    pub entry_status: EntryStatus,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub from: usize,
    pub term: usize,
    // the index and term of the last log
    pub last_log: LogInfo,
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
    pub vote: VoteStatus,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QueryEntryArgs {
    pub term: usize,
    pub log_info: LogInfo,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryEntryReply {
    Exist,
    NotExist,
}
pub type QueryEntryRes = Result<QueryEntryReply, ()>;

pub struct RpcService {
    raft: RaftHandle,
}

impl RpcService {
    pub fn new(raft: RaftHandle) -> Self {
        Self { raft }
    }
}

#[macros::rpc]
impl RpcService {
    /// The AppendEntries event may not be processed.
    /// For instance, the Raft node may already be dead.
    pub async fn append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesRes {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::AppendEntries { args, reply_tx: tx };
        if let Err(_) = self.raft.ev_q.just_put(ev) {
            return Err(());
        }
        rx.await.map_err(|_| ())
    }

    /// Request a vote from this raft node.
    pub async fn request_vote(&self, args: RequestVoteArgs) -> RequestVoteRes {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::RequestVote { args, reply_tx: tx };
        if let Err(_) = self.raft.ev_q.just_put(ev) {
            return Err(());
        }
        rx.await.map_err(|_| ())
    }

    pub async fn query_entry(&self, args: QueryEntryArgs) -> QueryEntryRes {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::QueryEntry { args, reply_tx: tx };
        if let Err(_) = self.raft.ev_q.just_put(ev) {
            return Err(());
        }
        rx.await.map_err(|_| ())
    }
}

impl std::fmt::Display for AppendEntriesType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeartBeat => write!(f, "HeartBeat"),
            Self::Entries { prev, entries } => {
                write!(f, "AppendEntries[{prev}, {} entries]", entries.len())
            },
            Self::Snapshot {
                last_log_idx,
                last_log_term,
                snapshot_lii,
                ..
            } => {
                write!(f, "Snapshot[{}, {}, {}]", 
                    last_log_idx,
                    last_log_term,
                    snapshot_lii)
            }
        }
    }
}
impl std::fmt::Debug for AppendEntriesType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeartBeat => write!(f, "HeartBeat"),
            Self::Entries { prev, entries } => {
                write!(
                    f,
                    "AppendEntries {{prev: {prev}, entries: [{}, {}]}}",
                    entries.first().unwrap().index,
                    entries.last().unwrap().index
                )
            },
            Self::Snapshot {
                last_log_idx,
                last_log_term,
                snapshot_lii,
                ..
            } => {
                write!(f, "Snapshot[{}, {}, {}]", 
                    last_log_idx,
                    last_log_term,
                    snapshot_lii)
            }
        }
    }
}

impl std::fmt::Display for AppendEntriesArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} from {}, term = {}",
            self.entry_type, self.from, self.term
        )
    }
}

impl std::fmt::Display for RequestVoteArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RequestVote from {}, term = {}", self.from, self.term)
    }
}
