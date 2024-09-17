// Date:   Sat Sep 07 15:50:26 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{ops::Range, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{raft::RaftCore, service::AppendEntriesType};

#[derive(Clone, Serialize, Deserialize)]
pub struct LogInfo {
    pub index: usize,
    pub term: usize
}

/// LogType can be:
/// 1. Noop: just for pushing in a log entry;
#[derive(Clone, Serialize, Deserialize)]
pub enum LogType {
    // Cmd {
    //     content: Vec<u8>,
    //     // each command has an unique index,
    //     // which is different from the log index.
    //     index: usize
    // },
    Noop
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: usize,
    pub term: usize,
    pub log_type: LogType
}

/// A LogPack can be
/// 1. A list of entries
/// 2. A compressed snapshot
pub enum LogPack {
    Entries(Vec<LogEntry>),
}

/// A list of log entries, actually a wrapper of Vec<LogEntry>.
/// As with snapshot, the first log in the list may not be the 
/// actual first log of all time. To implement a better std::ops::Index
#[derive(Default)]
struct LogList {
    logs: Vec<LogEntry>,
    offset: usize
}

struct LogsImpl {
    // last log index
    // lli: Arc<AtomicUsize>,
    // last committed log index
    // lci: Arc<AtomicUsize>,
    // last applied log index
    // lai: Arc<AtomicUsize>,

    // logs is ensured to be always not empty
    logs: RwLock<LogList>,
    core: RaftCore
}

pub type Logs = Arc<LogsImpl>;

impl LogsImpl {
    const EMPTY_LOGS: &'static str = "Logs log list should not be empty";

    pub fn new(core: RaftCore) -> Arc<Self> {
        Arc::new(Self {
            // lli: Default::default(),
            // lci: Default::default(),
            // lai: Default::default(),
            logs: {
                let mut logs = LogList::default();
                logs.push(LogEntry { index: 0, term: 0, log_type: LogType::Noop });
                RwLock::new(logs)
            },
            core
        })
    }

    pub async fn last_log_info(&self) -> LogInfo {
        self.logs.read().await
            .logs
            .last()
            .map(|log| LogInfo {
                index: log.index,
                term: log.term
            })
            .expect(Self::EMPTY_LOGS)
    }

    // push is for a leader to push an new entry to logs.
    pub async fn push(&self, entry: LogEntry) {
        let mut logs = self.logs.write().await;
        logs.push(entry);
    }

    // check_push is for a follower to push an entry replicated from a leader 
    // to logs, with the previous log info checked.
    pub async fn try_push(&self, prev_entry_info: &LogInfo, entries: 
        Vec<LogEntry>) -> Result<(), Vec<LogEntry>> 
    {
        let mut log_list = self.logs.write().await;
        let my_last_index = log_list.logs.last().map(|entry| entry.index)
            .expect(Self::EMPTY_LOGS);

        if prev_entry_info.index < my_last_index {
            let _ = log_list.logs.drain(prev_entry_info.index..);
            log_list.extend(entries);
            Ok(())
        } else {
            Err(entries)
        }
    }

    pub async fn up_to_date(&self, log_info: &LogInfo) -> bool {
        let my_last = self.last_log_info().await;
        
        if my_last.term == log_info.term {
            my_last.index <= log_info.index
        } else {
            my_last.term < log_info.term
        }
    }

    /// A service provide for replicators
    pub async fn repl_get(&self, next_index: usize) -> AppendEntriesType {
        let logs = self.logs.read().await;
        let lli = logs.last()
            .map(|entry| entry.index)
            .expect(Self::EMPTY_LOGS);
        
        assert!(((logs.offset+1)..=lli).contains(&next_index));
        if next_index == lli {
            AppendEntriesType::HeartBeat
        } else {
            AppendEntriesType::Entries {
                prev: logs.get(next_index-1)
                    .map(LogInfo::from)
                    .expect(&format!("Invalid next_index {next_index}")),
                entries: logs.logs.get(next_index..).unwrap().to_vec()
            }
        }
    }

    pub async fn lli(&self) -> usize {
        self.logs.read().await.last().unwrap().index
    }

    pub async fn lii_lli(&self) -> (usize, usize) {
        let logs = self.logs.read().await;
        (logs.offset, logs.last().unwrap().index)
    }

    // get a log entry term with the log index,
    // return None if the entry is already packed in snapshot.
    pub async fn index_term(&self, index: usize) -> Option<usize> {
        let logs = self.logs.read().await;
        if index < logs.offset {
            None
        } else {
            logs.get(index - logs.offset)
                .map(|entry| entry.term)
        }
    }

    // #[inline]
    // pub fn lli(&self) -> usize {
    //     self.lli.load(Ordering::Acquire)
    // }
    // #[inline]
    // pub fn lci(&self) -> usize {
    //     self.lci.load(Ordering::Acquire)
    // }
    // #[inline]
    // pub fn lai(&self) -> usize {
    //     self.lai.load(Ordering::Acquire)
    // }
}

impl LogInfo {
    pub fn new(index: usize, term: usize) -> Self {
        LogInfo { index, term }
    }
}

impl LogList {
    fn push(&mut self, entry: LogEntry) {
        self.logs.push(entry);
    }

    fn extend(&mut self, entries: Vec<LogEntry>) {
        self.logs.extend(entries);
    }

    fn get(&self, index: usize) -> Option<&LogEntry> {
        if index < self.offset {
            None
        } else {
            self.logs.get(index - self.offset)
        }
    }

    fn last(&self) -> Option<&LogEntry> {
        self.logs.last()
    }
}

impl From<&LogEntry> for LogInfo {
    fn from(entry: &LogEntry) -> Self {
        Self {
            index: entry.index,
            term: entry.term
        }
    }
}
