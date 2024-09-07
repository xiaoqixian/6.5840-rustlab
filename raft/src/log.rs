// Date:   Sat Sep 07 15:50:26 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

use serde::{Deserialize, Serialize};

use crate::raft::RaftCore;

#[derive(Clone, Serialize, Deserialize)]
pub struct LogEntry {
    index: usize,
    term: usize,
    command: Vec<u8>
}

struct Logs_ {
    // last log index
    lli: Arc<AtomicUsize>,
    // last committed log index
    lci: Arc<AtomicUsize>,
    // last applied log index
    lai: Arc<AtomicUsize>,

    logs: Vec<LogEntry>,
    core: RaftCore
}

pub type Logs = Arc<Logs_>;

impl Logs_ {
    #[inline]
    pub fn lli(&self) -> usize {
        self.lli.load(Ordering::Acquire)
    }
    #[inline]
    pub fn lci(&self) -> usize {
        self.lci.load(Ordering::Acquire)
    }
    #[inline]
    pub fn lai(&self) -> usize {
        self.lai.load(Ordering::Acquire)
    }
}
