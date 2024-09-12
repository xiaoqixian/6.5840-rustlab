// Date:   Fri Sep 06 16:10:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{candidate::VoteStatus, event::Event, raft::RaftCore};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
enum Entry {
    HeartBeat
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub id: usize,
    pub term: usize,
    pub leader_commit: usize,
    pub entry: Entry
}
#[derive(Serialize, Deserialize)]
pub struct AppendEntriesReply {}

#[derive(Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub id: usize,
    pub term: usize,
    // the index and term of the last log
    pub last_log: (usize, usize)
}
#[derive(Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub voter: usize,
    // term represents the term of the request that 
    // this reply response to.
    // Without term, the receiver may confuse earlier term 
    // responses with current term responses, and cause vote 
    // inconsistent.
    pub term: usize,
    pub vote: VoteStatus
}

pub struct RpcService {
    core: RaftCore
}

#[labrpc_macros::rpc]
impl RpcService {
    /// The AppendEntries event may not be processed.
    /// For instance, the Raft node may already be dead.
    pub async fn append_entries(&self, args: AppendEntriesArgs) -> 
        Result<AppendEntriesReply, ()> {
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
    pub async fn request_vote(&self, args: RequestVoteArgs) -> 
        Result<RequestVoteReply, ()> {
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
}
