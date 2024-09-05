// Date:   Thu Sep 05 11:00:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::raft::RaftCore;

pub struct Candidate {
    pub(crate) core: RaftCore,
    pub(crate) term: usize
}
