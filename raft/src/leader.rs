// Date:   Thu Sep 05 11:00:39 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{candidate::Candidate, event::Event, log::Logs, raft::RaftCore, role::Trans};

pub struct Leader {
    pub core: RaftCore,
    pub logs: Logs
}

impl Leader {
    pub async fn process(&mut self, ev: Event) -> Option<Trans> {
        None
    }

    pub async fn stop(&mut self) {}
}

impl From<Candidate> for Leader {
    fn from(cd: Candidate) -> Self {
        Self {
            core: cd.core,
            logs: cd.logs
        }
    }
}
