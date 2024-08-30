// Date:   Thu Aug 29 16:27:57 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

#[cfg(test)]
mod test_3a;

use std::{collections::HashMap, sync::Arc};

use labrpc::network::Network;
use tokio::sync::Mutex;
use crate::{msg::{ApplyMsg, Command}, Rx};

struct Config {
    n: usize,
    net: Network,
    bytes: usize,
    last_applied: Vec<usize>,
    // logs[i] represent log entries applied by node i,
    // logs[i][j] represent the command value, 
    // we use u32 to represent a command in tests.
    logs: Arc<Vec<Vec<u32>>>,
    lock: Arc<Mutex<()>>,
    max_cmd_indx: usize,
}

struct Logs {
    logs: Vec<Vec<u32>>,
    max_cmd_indx: usize
}

struct Applier {
    id: u32,
    logs: Arc<Mutex<Logs>>,
    apply_ch: Rx<ApplyMsg>
}

impl Config {
    // pub async fn new(n: usize, unreliable: bool, snapshot: bool) -> Self {
    //     let net = Network::new();
    // }

    async fn start1()
}

impl Applier {
    async fn run(mut self) {
        while let Some(msg) = self.apply_ch.recv().await {
            match msg {
                ApplyMsg::Command(cmd) => {
                    
                },
                ApplyMsg::Snapshot(snapshot) => {}
            }
        }
    }

    /// Check applied commands index and term consistency,
    /// if ok, insert this command into logs.
    /// WARN: check_logs assume the Config.lock is hold by the caller.
    async fn check_logs(&mut self, id: u32, cmd: Command) {
        let cmd_value = bincode::deserialize::<u32>(&cmd.command[..])
            .expect("Expected command value type to be u32");

        let mut logs_ctrl = self.logs.lock().await;
        let logs = &mut logs_ctrl.logs;

        if cmd.index > logs[id as usize].len() {
            panic!("server {id} apply out of order {}", cmd.index);
        }

        for i in 0..logs.len() {
            let log = &logs[i];
            match log.get(cmd.index) {
                Some(&val) if val != cmd_value => {
                    panic!("commit index = {} server={} {} != server={} {}",
                        cmd.index, 
                        id, cmd_value,
                        i, val);
                },
                _ => {}
            }
        }

        logs[id as usize].push(cmd_value);
        logs_ctrl.max_cmd_indx = usize::max(logs_ctrl.max_cmd_indx, 
            cmd.index);
    }
}
