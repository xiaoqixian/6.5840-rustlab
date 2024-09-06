// Date:   Thu Sep 05 11:00:39 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::raft::RaftCore;

pub struct Leader {
    pub(crate) core: Arc<RaftCore>,
}

impl Leader {
    pub fn new(core: Arc<RaftCore>) -> Self {
        Self { core }
    }

    pub async fn process(&mut self, ev: Event) -> Option<Outcome> {
        None
    }
}
