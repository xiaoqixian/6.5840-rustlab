// Date:   Thu Sep 05 09:41:37 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use crate::{raft::{Raft, RaftCore}, utils::{self, HEARTBEAT_TIMEOTU}};



pub struct Follower {
    pub(crate) core: Arc<RaftCore>,
    pub(crate) term: usize
}

impl Follower {
    pub fn new(core: Arc<RaftCore>) -> Self {
        Self {
            core,
            term: 0
        }
    }
}
