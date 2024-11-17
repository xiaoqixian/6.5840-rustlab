// Date:   Tue Oct 01 16:04:44 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::ops::{RangeBounds, RangeInclusive};

use serde::{Deserialize, Serialize};

use crate::{debug, info};

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
    snapshot: Option<(usize, Vec<u8>)>
}

/// LogsInfo contains essential information needed for Logs to recover itself.
#[derive(Serialize, Deserialize)]
pub struct LogsInfo {
    offset: usize,
    lci: usize,
    cmd_cnt: usize,
    logs: Vec<LogEntry>,
    snapshot_lii: Option<usize>
}

impl Logs {
    pub fn new(
        me: usize, 
        logs_info: LogsInfo,
        snapshot: Option<Vec<u8>>
    ) -> Self {
        let LogsInfo { lci, logs, offset, cmd_cnt, snapshot_lii } = logs_info;

        debug_assert!(
            logs[0].index == 0 || (snapshot.is_some() && snapshot_lii.is_some()), 
            "Logs[{me}]: logs start at {}, but the snapshot is None", 
            logs[0].index
        );

        Self {
            me,
            logs,
            offset,
            cmd_cnt,
            lci,
            snapshot: snapshot_lii.zip(snapshot)
        }
    }

    pub fn lci(&self) -> usize {
        self.lci
    }

    pub fn lli(&self) -> usize {
        self.last_log_info().index
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

    pub fn index_term(&self, index: usize) -> Option<usize> {
        self.get(index)
            .map(|entry| entry.term)
    }

    pub fn index_cmd(&self, cmd_idx: usize) -> Option<usize> {
        self.logs.iter()
            .find_map(|et| match et.log_type {
                LogType::Command {index, ..} if index == cmd_idx => Some(et.index),
                _ => None
            })
    }

    pub fn log_exist(&self, log_info: &LogInfo) -> bool {
        let idx = if log_info.index <= self.offset {
            return true;
        } else { log_info.index - self.offset };

        self.logs
            .get(idx)
            .map(|et| et.term == log_info.term)
            .or(Some(false))
            .unwrap()
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        (index >= self.offset)
            .then(|| self.logs.get(index - self.offset))
            .flatten()
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
        // We may receive delayed requests in a terrible network environment,
        // in suce case we should just ignore the request.
        // If the prev.index < lci, it means this is a delayed request, 
        // so it gets ignored.
        if prev.index < self.lci {
            return Ok(());
        }

        debug_assert!(self.lci >= self.offset);
        let diff = prev.index - self.offset;

        if let None = self.logs.get(diff) {
            debug!("{self}: reject merge cause prev.index {} does not exist in logs", prev.index);
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
        let lli = self.lli();
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
        let lli = self.lli();
        self.logs.push(LogEntry {
            index: lli + 1,
            term,
            log_type: LogType::Noop
        });
        info!("{self}: push new noop at log index {}", lli+1);
        lli + 1
    }

    /// update LCI, return all the command logs between the old lci 
    /// and the new lci.
    pub fn update_commit(&mut self, lci: usize) -> Vec<(usize, Vec<u8>)> {
        if lci <= self.lci || self.logs.is_empty() {
            return Vec::new();
        }

        let lci = lci.min(self.logs.last().unwrap().index);
        let apply_range = (self.lci+1)..=lci;
        self.lci = lci;

        match self.get_range(&apply_range) {
            None => panic!("{self}: Invalid range {apply_range:?}, 
                expected logs range {:?}", self.logs_range()),
            Some(ets) => ets.into_iter().cloned()
                .filter_map(LogEntry::into)
                .collect()
        }
    }

    pub fn logs_range(&self) -> RangeInclusive<usize> {
        let f = self.logs.first().unwrap().index;
        let l = self.logs.last().unwrap().index;
        f..=l
    }

    pub fn take_snapshot(&mut self, cmd_idx: usize, snap: Vec<u8>) {
        let (last_log_idx, last_log_term) = self.logs.iter()
            .find_map(|et| match et.log_type {
                LogType::Command {index, ..} if index == cmd_idx => Some((et.index, et.term)),
                _ => None
            })
            .expect(&format!("Cannot find a log entry with command \
                    index = {cmd_idx}"));

        self.snapshot = Some((cmd_idx, snap));
        
        // remove logs contained in the snapshot
        let end = last_log_idx - self.offset;
        self.logs.drain(1..=end);
        self.offset = last_log_idx;
        self.logs[0] = LogEntry {
            index: last_log_idx, 
            term: last_log_term,
            log_type: LogType::Noop
        };

        #[cfg(not(feature = "no_debug"))]
        {
            let range = match (
                self.logs.first().map(|e| e.index),
                self.logs.last().map(|e| e.index)
            ) {
                (Some(f), Some(l)) => format!("{:?}", f..=l),
                _ => "..".to_string()
            };
            debug!("{self}: take a snapshot, offset = {}, logs range = {range}", self.offset);
        }
    }

    pub fn snapshot(&self) -> Option<&(usize, Vec<u8>)> {
        self.snapshot.as_ref()
    }

    pub fn install_snapshot(
        &mut self,
        last_log_idx: usize,
        last_log_term: usize,
        snapshot_lii: usize,
        snapshot: Vec<u8>,
    ) {
        self.logs = vec![LogEntry {
            index: last_log_idx,
            term: last_log_term,
            log_type: LogType::Noop
        }];
        self.offset = last_log_idx;
        self.cmd_cnt = snapshot_lii + 1;
        self.lci = last_log_idx;
        debug!("{self}: install a snapshot, new offset = {}, new cmd_cnt = {}", self.offset, self.cmd_cnt);
        self.snapshot = Some((snapshot_lii, snapshot));
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
        write!(f, "Logs[{}]", self.me)
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
            lci: 0,
            snapshot_lii: None
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
        let mut s = serializer.serialize_struct("LogsInfo", 5)?;
        s.serialize_field("offset", &self.offset)?;
        s.serialize_field("lci", &self.lci)?;
        s.serialize_field("cmd_cnt", &self.cmd_cnt)?;
        s.serialize_field("logs", &self.logs)?;
        s.serialize_field("snapshot_lii", 
            &self.snapshot.as_ref().map(|(lii, _)| *lii))?;
        s.end()
    }
}
