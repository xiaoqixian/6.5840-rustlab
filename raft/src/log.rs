// Date:   Sat Sep 07 15:50:26 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::raft::RaftCore;

#[derive(Clone, Serialize, Deserialize)]
pub enum LogType {
    Cmd(Vec<u8>),
    Noop
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LogEntry {
    index: usize,
    term: usize,
    log: LogType
}

struct LogsImpl {
    // last log index
    lli: Arc<AtomicUsize>,
    // last committed log index
    lci: Arc<AtomicUsize>,
    // last applied log index
    lai: Arc<AtomicUsize>,

    // logs is ensured to be always not empty
    logs: RwLock<Vec<LogEntry>>,
    core: RaftCore
}

#[derive(Clone)]
pub struct Logs(Arc<LogsImpl>);

impl Logs {
    pub fn new(core: RaftCore) -> Self {
        Self(Arc::new(LogsImpl {
            lli: Default::default(),
            lci: Default::default(),
            lai: Default::default(),
            logs: RwLock::new(
                vec![LogEntry {
                    index: 0,
                    term: 0,
                    log: LogType::Noop
                }]
            ),
            core
        }))
    }

    pub async fn last_log_info(&self) -> (usize, usize) {
        self.0.logs.read().await
            .last()
            .map(|log| (log.index, log.term))
            .expect("Logs logs should not be empty")
    }

    #[inline]
    pub fn lli(&self) -> usize {
        self.0.lli.load(Ordering::Acquire)
    }
    #[inline]
    pub fn lci(&self) -> usize {
        self.0.lci.load(Ordering::Acquire)
    }
    #[inline]
    pub fn lai(&self) -> usize {
        self.0.lai.load(Ordering::Acquire)
    }
}
