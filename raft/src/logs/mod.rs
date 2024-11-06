// Date:   Tue Oct 01 16:04:44 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::ops::{RangeBounds, RangeInclusive};

use applier::{Applier, ApplyEntry};
use serde::{Deserialize, Serialize};

use crate::{fatal, info, warn, ApplyMsg, UbTx};

mod applier;

#[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct LogInfo {
    pub index: usize,
    pub term: usize
}

/// LogType can be:
/// 1. Noop: just for pushing in a log entry;
#[derive(Clone, Serialize, Deserialize)]
pub enum LogType {
    Command {
        command: Vec<u8>,
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

pub struct Logs {
    me: usize,
    logs: Vec<LogEntry>,
    offset: usize,
    cmd_cnt: usize,
    lci: usize,
    apply_tx: UbTx<ApplyEntry>,
}

/// LogsInfo contains essential information needed for Logs to recover itself.
#[derive(Serialize, Deserialize)]
pub struct LogsInfo {
    offset: usize,
    lci: usize,
    cmd_cnt: usize,
    logs: Vec<LogEntry>,
}

impl Logs {
    pub fn new(
        me: usize, 
        apply_tx: UbTx<ApplyMsg>,
        logs_info: LogsInfo,
        lai: Option<usize>
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(Applier::new(me, apply_tx).start(rx));

        let LogsInfo { lci, logs, offset, cmd_cnt } = logs_info;

        if let Some(range) = match lai {
            Some(lai) if lai < cmd_cnt - 1 => {
                let start = logs.iter().enumerate()
                    .find_map(|(idx, et)| match et.log_type {
                        LogType::Command {index, ..} if index == lai+1 => Some(idx),
                        _ => None
                    })
                    .expect(&format!("Command with index {} \
                            should exist in logs.", lai+1));
                Some(start..logs.len())
            },
            _ => None
        } {
            let entries = logs.get(range).unwrap()
                .into_iter().cloned()
                .filter_map(LogEntry::into)
                .collect();
            if let Err(_) = tx.send(ApplyEntry::Entries { entries }) {
                warn!("Applier channel should not be closed so soon");
            }
        }

        Self {
            me,
            logs,
            offset,
            cmd_cnt,
            lci,
            apply_tx: tx,
        }
    }

    pub fn lci(&self) -> usize {
        self.lci
    }

    pub fn lli(&self) -> usize {
        self.logs.last().unwrap().index
    }

    pub fn last_log_info(&self) -> LogInfo {
        self.logs.last()
            .map(LogInfo::from)
            .unwrap()
    }

    /// If a log is at least as new as the last log in the list.
    pub fn up_to_date(&self, log: &LogInfo) -> bool {
        self.last_log_info() <= *log
    }

    pub fn _cmd_cnt(&self) -> usize {
        self.cmd_cnt
    }

    pub fn index_term(&self, index: usize) -> Option<usize> {
        self.get(index)
            .map(|entry| entry.term)
    }

    pub fn log_exist(&self, log_info: &LogInfo) -> bool {
        let idx = if log_info.index < self.offset {
            return true;
        } else { log_info.index - self.offset };
        match self.logs.get(idx) {
            None => false,
            Some(et) => et.term == log_info.term
        }
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        if index < self.offset {
            None
        } else {
            self.logs.get(index - self.offset)
        }
    }

    pub fn get_range<R>(&self, range: &R) -> Option<&[LogEntry]>
        where R: RangeBounds<usize> + std::fmt::Debug
    {
        use std::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(&start) => start - self.offset,
            Bound::Excluded(&start) => start - self.offset + 1,
            Bound::Unbounded => 0
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end - self.offset + 1,
            Bound::Excluded(&end) => end - self.offset,
            Bound::Unbounded => self.logs.len()
        };
        info!("{self}: get_range, logs len = {}, query_range = {:?}, get range = {:?}", self.logs.len(), range, start..end);
        self.logs.get(start..end)
    }

    /// Try to merge some log entries to the list, requires the prev loginfo 
    /// must match any of the log in the list. 
    /// If matched, remove all logs after prev if there are any, then 
    /// extend entries in the tail.
    /// Otherwise, return Err(entries).
    pub fn try_merge(&mut self, prev: &LogInfo, entries: Vec<LogEntry>) -> Result<(), Vec<LogEntry>>{
        let diff = if prev.index < self.offset {
            return Err(entries);
        } else { prev.index - self.offset };

        if let None = self.logs.get(diff) {
            return Err(entries);
        }
        if diff+1 < self.logs.len() {
            info!("{self}: removed logs range {:?}", (prev.index+1)..);
        }

        fn count_cmd(acc: usize, et: &LogEntry) -> usize {
            match &et.log_type {
                LogType::Command {..} => acc + 1,
                LogType::Noop => acc
            }
        }
        // I know this is kind of dangerous, 
        // but I don't want to use an extra block.
        self.cmd_cnt -= self.logs.drain((diff+1)..)
            .as_ref()
            .iter()
            .fold(0, count_cmd);

        self.cmd_cnt += entries.iter().fold(0, count_cmd);

        self.logs.extend(entries);
        Ok(())
    }

    /// Push a command entry to logs, return the entry index and the command index.
    pub fn push_cmd(&mut self, term: usize, cmd: Vec<u8>) -> (usize, usize) {
        let lli = self.logs.last().unwrap().index;
        let cmd_idx = self.cmd_cnt;
        self.cmd_cnt += 1;
        self.logs.push(LogEntry {
            index: lli + 1,
            term,
            log_type: LogType::Command {
                command: cmd,
                index: cmd_idx
            }
        });
        info!("{self}: push new command with index {cmd_idx} at log index {}", lli+1);
        (lli + 1, cmd_idx)
    }

    pub fn push_noop(&mut self, term: usize) -> usize {
        let lli = self.logs.last().unwrap().index;
        self.logs.push(LogEntry {
            index: lli + 1,
            term,
            log_type: LogType::Noop
        });
        info!("{self}: push new noop at log index {}", lli+1);
        lli + 1
    }

    /// Update last committed index, this will cause the logs between
    /// [old_lci+1, new_lci] applied.
    pub fn update_commit(&mut self, lci: usize) {
        if lci <= self.lci {
            return;
        }

        let lci = lci.min(self.logs.last().unwrap().index);
        let apply_range = (self.lci+1)..=lci;
        self.lci = lci;

        if self.apply_tx.is_closed() {
            return
        }

        let entries = match self.get_range(&apply_range) {
            None => fatal!("{self}: Invalid range {apply_range:?}, 
                expected logs range {:?}", self.logs_range()),
            Some(ets) => ets.into_iter().cloned()
                .filter_map(LogEntry::into)
                .collect()
        };

        if let Ok(_) = self.apply_tx.send(ApplyEntry::Entries {entries}) {
            info!("{self}: apply logs range {apply_range:?}");
        }
    }

    pub fn logs_range(&self) -> RangeInclusive<usize> {
        let len = self.logs.len();
        self.offset..=(self.offset + len - 1)
    }
}
impl LogInfo {
    pub fn new(index: usize, term: usize) -> Self {
        LogInfo { index, term }
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

/// Convert a LogEntry into a command
impl Into<Option<(usize, Vec<u8>)>> for LogEntry {
    fn into(self) -> Option<(usize, Vec<u8>)> {
        match self.log_type {
            LogType::Command { command, index } => Some((index, command)),
            _ => None
        }
    }
}

impl std::fmt::Display for Logs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Logs {}]", self.me)
    }
}

impl std::default::Default for LogsInfo {
    fn default() -> Self {
        Self {
            logs: vec![LogEntry {
                index: 0,
                term: 0,
                log_type: LogType::Noop
            }],
            offset: 0,
            cmd_cnt: 0,
            lci: 0
        }
    }
}

impl std::fmt::Debug for LogsInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, 
r"LogsInfo {{
    offset: {},
    lci: {},
    cmd_cnt: {},
    logs: [{}..={}]
}}", 
            self.offset,
            self.lci,
            self.cmd_cnt,
            self.logs.first().unwrap().index,
            self.logs.last().unwrap().index
        )
    }
}

impl Serialize for Logs {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("LogsInfo", 4)?;
        s.serialize_field("offset", &self.offset)?;
        s.serialize_field("lci", &self.lci)?;
        s.serialize_field("cmd_cnt", &self.cmd_cnt)?;
        s.serialize_field("logs", &self.logs)?;
        s.end()
    }
}
