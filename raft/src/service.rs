// Date:   Fri Sep 06 16:10:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{candidate::VoteStatus, event::{EvQueue, Event}, raft::RaftCore};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
enum Entry {
    HeartBeat
}

#[derive(Serialize, Deserialize)]
pub(crate) struct AppendEntriesArgs {
    pub(crate) id: usize,
    pub(crate) term: usize,
    pub(crate) leader_commit: usize,
    pub(crate) entry: Entry
}
#[derive(Serialize, Deserialize)]
pub(crate) struct AppendEntriesReply {}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RequestVoteArgs {
    pub(crate) id: usize,
    pub(crate) term: usize,
    // the index and term of the last log
    pub(crate) last_log: (usize, usize)
}
#[derive(Serialize, Deserialize)]
pub(crate) struct RequestVoteReply {
    id: usize,
    vote: VoteStatus
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
        self.core.ev_q.must_put(ev).await
            .expect("Event putting failed for multiple times, which 
                is not expected");
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
        self.core.ev_q.must_put(ev).await
            .expect("Event putting failed for multiple times, which 
                is not expected");
        Ok(rx.await.unwrap())
    }
}
