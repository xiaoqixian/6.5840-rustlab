// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use crate::{event::Event, raft::RaftCore, Outcome};

pub struct Candidate {
    pub(crate) core: Arc<RaftCore>,
}

impl Candidate {
    pub fn new(core: Arc<RaftCore>) -> Self {
        Self { core }
    }

    pub async fn process(&mut self, ev: Event) -> Option<Outcome> {
        None
    }
}
