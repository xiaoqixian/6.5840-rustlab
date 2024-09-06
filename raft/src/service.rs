// Date:   Fri Sep 06 16:10:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::event::EvQueue;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct AppendEntriesArgs {
    id: usize,
    term: usize,
    leader_commit: usize,

}

#[derive(Serialize, Deserialize)]
struct AppendEntriesReply {}

pub struct RpcService {
    ev_q: EvQueue
}

#[labrpc_macros::rpc]
impl RpcService {
    pub async fn append_entries(&self, args: AppendEntriesArgs) -> AppendEntriesReply {
        AppendEntriesReply {}
    }
}
