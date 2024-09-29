// Date:   Sat Sep 07 15:50:26 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::raft::RaftCore;

#[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct LogInfo {
    pub index: usize,
    pub term: usize
}

/// LogType can be:
/// 1. Noop: just for pushing in a log entry;
#[derive(Clone, Serialize, Deserialize)]
pub enum LogType {
    Cmd {
        content: Vec<u8>,
        // each command has an unique index,
        // which is different from the log index.
        index: usize
    },
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
// pub enum LogPack {
//     Entries(Vec<LogEntry>),
// }

/// A list of log entries, actually a wrapper of Vec<LogEntry>.
/// As with snapshot, the first log in the list may not be the 
/// actual first log of all time. To implement a better std::ops::Index
#[derive(Default)]
struct LogList {
    logs: Vec<LogEntry>,
    offset: usize,
    // record the number of commands in the log list, including the ones 
    // in the snapshot. So the command indices returned will be consecutive.
    cmd_cnt: usize
}

#[derive(Clone)]
pub struct Logs {
    // last log index
    // lli: Arc<AtomicUsize>,
    // last committed log index
    // lci: Arc<AtomicUsize>,
    // last applied log index
    // lai: Arc<AtomicUsize>,

    // logs is ensured to be always not empty
    logs: Arc<RwLock<LogList>>
}

impl Logs {
    const EMPTY_LOGS: &'static str = "Logs log list should not be empty";

    pub fn new() -> Self {
        Self {
            // lli: Default::default(),
            // lci: Default::default(),
            // lai: Default::default(),
            logs: {
                let mut logs = LogList::default();
                logs.push(LogEntry { index: 0, term: 0, log_type: LogType::Noop });
                Arc::new(RwLock::new(logs))
            },
        }
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
        let lli = log_list.logs.last().map(|entry| entry.index)
            .expect(Self::EMPTY_LOGS);

        if prev_entry_info.index < lli {
            let _ = log_list.logs.drain(prev_entry_info.index..);
            log_list.extend(entries);
            Ok(())
        } else {
            Err(entries)
        }
    }

    pub async fn ld_push_noop(&self, term: usize) {
        let mut logs = self.logs.write().await;
        let lli = logs.last().unwrap().index;
        logs.push(LogEntry {
            index: lli + 1,
            term,
            log_type: LogType::Noop
        });
    }

    /// Push a cmd entry to the list, return the cmd index.
    pub async fn ld_push_cmd(&self, term: usize, cmd: Vec<u8>) -> usize {
        let mut logs = self.logs.write().await;
        let lli = logs.last().unwrap().index;
        let cmd_idx = logs.cmd_cnt;
        logs.cmd_cnt += 1;
        logs.push(LogEntry {
            index: lli + 1,
            term,
            log_type: LogType::Cmd {
                content: cmd,
                index: cmd_idx
            }
        });
        cmd_idx
    }

    pub async fn up_to_date(&self, log_info: &LogInfo) -> bool {
        let my_last = self.last_log_info().await;
        my_last <= *log_info
    }

    /// Called by replicators, given a next_index, if the latest log 
    /// index is greater than next_index, then there are some new logs,
    /// return Some((prev, entries)) if there are new logs, where prev is 
    /// the log info in the previous pos of next_index, entries is a list 
    /// of entries in range [next_index, lli].
    /// return None if there are no new logs.
    pub async fn repl_get(&self, next_index: usize) -> Option<(LogInfo, Vec<LogEntry>)> {
        let logs = self.logs.read().await;
        let lli = logs.last()
            .map(|entry| entry.index)
            .expect(Self::EMPTY_LOGS);
        
        assert!(((logs.offset+1)..=lli).contains(&next_index), 
            "next_index {next_index} not found in [{}, {lli}]", logs.offset+1);

        if next_index == lli {
            None
            // AppendEntriesType::HeartBeat
        } else {
            Some((
                logs.get(next_index-1)
                    .map(LogInfo::from)
                    .expect(&format!("Invalid next_index {next_index}")),
                logs.logs.get(next_index..).unwrap().to_vec()
            ))
        }
    }

    pub async fn lli(&self) -> usize {
        self.logs.read().await.last().unwrap().index
    }

    pub async fn lii_lli(&self) -> (usize, usize) {
        let logs = self.logs.read().await;
        (logs.offset + 1, logs.last().unwrap().index)
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

    pub async fn log_exist(&self, log_info: &LogInfo) -> bool {
        let logs = self.logs.read().await;
        if log_info.index < logs.offset {
            true
        } else {
            match logs.get(log_info.index) {
                None => false,
                Some(l) => l.term == log_info.term
            }
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

impl std::fmt::Display for LogInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.index, self.term)
    }
}

impl std::cmp::PartialOrd for LogInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp;
        match self.term.cmp(&other.term) {
            cmp::Ordering::Equal => Some(self.index.cmp(&other.index)),
            ord => Some(ord)
        }
    }
}

impl std::cmp::Ord for LogInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}
