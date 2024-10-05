// Date:   Tue Oct 01 16:30:30 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::ops::{Range, RangeBounds};

use super::{LogEntry, LogInfo, LogType};

pub struct Logs {
    logs: Vec<LogEntry>,
    offset: usize,
    cmd_cnt: usize
}

impl Logs {
    pub fn new() -> Self {
        let mut logs = Vec::new();
        logs.push(LogEntry {
            index: 0,
            term: 0,
            log_type: LogType::Noop
        });

        Self {
            logs,
            offset: 0,
            cmd_cnt: 0
        }
    }

    pub fn last(&self) -> Option<&LogEntry> {
        self.logs.last()
    }

    pub fn lii(&self) -> usize {
        self.offset + 1
    }

    pub fn lli(&self) -> usize {
        self.logs.last().unwrap().index
    }

    pub fn index_term(&self, index: usize) -> Option<usize> {
        if index < self.offset {
            None
        } else {
            self.logs.get(index - self.offset)
                .map(|entry| entry.term)
        }
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

    pub fn get_range<R>(&self, range: &R) -> Option<&[LogEntry]>
        where R: RangeBounds<usize>
    {
        use std::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(&start) => start - self.offset,
            Bound::Excluded(&start) => start - self.offset + 1,
            Bound::Unbounded => 0
        };
        let end = match range.start_bound() {
            Bound::Included(&end) => end - self.offset + 1,
            Bound::Excluded(&end) => end - self.offset,
            Bound::Unbounded => self.logs.len()
        };
        self.logs.get(start..end)
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
        (lli + 1, cmd_idx)
    }

    pub fn push_noop(&mut self, term: usize) -> usize {
        let lli = self.logs.last().unwrap().index;
        self.logs.push(LogEntry {
            index: lli + 1,
            term,
            log_type: LogType::Noop
        });
        lli + 1
    }

    pub fn repl_get(&self, next_index: usize) -> Option<(LogInfo, Vec<LogEntry>)> {
        let lli = self.logs.last()
            .map(|entry| entry.index)
            .unwrap();
        
        assert!(((self.offset+1)..=lli).contains(&next_index), 
            "next_index {next_index} not found in [{}, {lli}]", self.offset+1);

        if next_index == lli {
            None
        } else {
            Some((
                self.logs.get(next_index-1)
                    .map(LogInfo::from)
                    .expect(&format!("Invalid next_index {next_index}")),
                self.logs.get(next_index..).unwrap().to_vec()
            ))
        }
        
    }
}
