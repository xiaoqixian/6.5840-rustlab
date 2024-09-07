// Date:   Thu Sep 05 11:00:39 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{candidate::Candidate, event::Event, raft::RaftCore, role::Trans};

pub struct Leader {
    pub(crate) core: RaftCore,
}

impl Leader {
    pub fn new(core: RaftCore) -> Self {
        Self { core }
    }

    pub async fn process(&mut self, ev: Event) -> Option<Outcome> {
        None
    }

    pub async fn stop(&mut self) {}
}

impl From<Candidate> for Leader {
    fn from(cd: Candidate) -> Self {
        let Candidate {
            core,
            ..
        } = cd;
        Self { core }
    }
}
