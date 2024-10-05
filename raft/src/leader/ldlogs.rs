// Date:   Fri Oct 04 14:48:03 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{ops::RangeInclusive, sync::{Arc, RwLock}};

use crate::{fatal, logs::{LogInfo, Logs}, service::AppendEntriesType};

/// LdLogs is a thread-safe wrapper of Logs
pub struct LdLogs {
    logs: Arc<RwLock<Option<Logs>>>
}

pub struct ReplLogs {
    logs: Arc<RwLock<Option<Logs>>>
}

pub struct ReplQueryRes {
    pub lci: usize,
    pub entry_type: AppendEntriesType
}

macro_rules! ld_logs_method {
    (read, $name: ident ( $($arg: ident : $arg_type: ty),* ) $(-> $ret_type: ty)?) => {
        pub fn $name(&self, $($arg: $arg_type),*) $(-> $ret_type)? {
            self.logs.read().unwrap().as_ref().unwrap()
                .$name($($arg),*)
        }
    };

    (write, $name: ident ( $($arg: ident : $arg_type: ty),* ) $(-> $ret_type: ty)?) => {
        pub fn $name(&self, $($arg: $arg_type),*) $(-> $ret_type)? {
            self.logs.write().unwrap().as_mut().unwrap()
                .$name($($arg),*)
        }
    };
}

impl LdLogs {
    ld_logs_method!(write, push_cmd(term: usize, cmd: Vec<u8>) -> (usize, usize));
    ld_logs_method!(write, push_noop(term: usize) -> usize);
    ld_logs_method!(read,  lci() -> usize);
    ld_logs_method!(read,  last_log_info() -> LogInfo);
    ld_logs_method!(write, update_commit(lci: usize));
    ld_logs_method!(read,  up_to_date(log: &LogInfo) -> bool);
    ld_logs_method!(read, log_exist(log: &LogInfo) -> bool);
}

/// All ReplLogs functions return a Result, Err represents that the Logs 
/// is no longer held by the leader.
impl ReplLogs {
    pub fn index_term(&self, index: usize) -> Result<Option<usize>, ()> {
        self.logs.read()
            .unwrap()
            .as_ref()
            .map(|logs| logs.index_term(index))
            .ok_or(())
    }

    pub fn logs_range(&self) -> Result<RangeInclusive<usize>, ()> {
        self.logs.read()
            .unwrap()
            .as_ref()
            .map(|logs| logs.logs_range())
            .ok_or(())
    }

    pub fn repl_get(&self, next_index: usize) -> Result<ReplQueryRes, ()> {
        debug_assert!(next_index >= 1);
        let logs_guard = self.logs.read().unwrap();
        let logs = match logs_guard.as_ref() {
            None => return Err(()),
            Some(l) => l
        };
        let (lci, lli) = (logs.lci(), logs.lli());
        
        let entry_type = if next_index > lli {
            AppendEntriesType::HeartBeat
        } else {
            // TODO: snapshot
            AppendEntriesType::Entries {
                prev: logs.get(next_index-1)
                    .map(LogInfo::from).unwrap(),
                entries: logs.get_range(&(next_index..)).unwrap().to_vec()
            }
        };
        
        Ok(ReplQueryRes {
            lci,
            entry_type
        })
    }
}

impl Into<Logs> for LdLogs {
    fn into(self) -> Logs {
        self.logs.write().unwrap()
            .take()
            .unwrap()
    }
}

impl From<Logs> for LdLogs {
    fn from(logs: Logs) -> Self {
        Self {
            logs: Arc::new(RwLock::new(Some(logs)))
        }
    }
}

impl From<&LdLogs> for ReplLogs {
    fn from(ld_logs: &LdLogs) -> Self {
        Self {
            logs: ld_logs.logs.clone()
        }
    }
}
