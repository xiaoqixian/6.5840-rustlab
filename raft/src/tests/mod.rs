// Date:   Thu Aug 29 16:27:57 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

mod utils;

#[cfg(test)]
mod test_3a;

#[cfg(test)]
mod test_3b;

#[cfg(test)]
mod test_3c;

use std::{collections::HashMap, fmt::{Debug, Display}, future::Future, ops::DerefMut, sync::{atomic::{AtomicBool, Ordering}, Arc}, time::Duration};

use labrpc::network::Network;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Mutex, RwLock};
use crate::{ApplyMsg, persist::Persister, raft::Raft, UbRx};
use colored::Colorize;

macro_rules! greet {
    ($($args: expr),*) => {{
        // let msg = format!($($args),*).truecolor(178,225,167);
        let msg = format!($($args),*).truecolor(135, 180, 106);
        println!("{msg}");
    }}
}

macro_rules! debug {
    ($($args: expr),*) => {
        #[cfg(not(feature = "no_test_debug"))]
        {
            let msg = format!("[CONFIG] {}", format_args!($($args),*))
                .truecolor(240, 191, 79);
            println!("{msg}");
        }
    }
}

const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const TEST_TIME_LIMIT: Duration = Duration::from_secs(120);

struct Node {
    // last_applied: usize,
    persister: Persister,
    raft: Option<Raft>,
    connected: bool
}

struct Config<T> {
    n: usize,
    net: Network,
    pub nodes: Vec<Node>,
    logs: Arc<Mutex<Logs<T>>>,
    
    start: std::time::Instant,
}

struct Logs<T> {
    logs: Vec<Vec<T>>,
    apply_err: Vec<Option<String>>,
    max_cmd_idx: usize
}

struct Applier<T> {
    id: usize,
    logs: Arc<Mutex<Logs<T>>>,
    apply_ch: UbRx<ApplyMsg>
}

/// T: the command type
#[derive(Clone)]
struct Tester<T> {
    config: Arc<RwLock<Config<T>>>,
    finished: Arc<AtomicBool>
}

trait WantedCmd: Eq + Clone + Serialize + DeserializeOwned + Display 
    + Debug + Send + 'static {}
impl<T> WantedCmd for T 
where T: Eq + Clone + Serialize + DeserializeOwned + Display 
    + Debug + Send + 'static {}

impl<T> Tester<T> 
    where T: WantedCmd
{
    pub async fn new(n: usize, reliable: bool, snapshot: bool) -> Result<Self, String> {
        let config = Config {
            n,
            net: Network::new(n).reliable(reliable).long_delay(true),
            logs: Arc::new(Mutex::new(Logs {
                logs: vec![Vec::new(); n],
                apply_err: vec![None; n],
                max_cmd_idx: 0
            })),
            nodes: std::iter::repeat_with(
                || Node {
                    // last_applied: 0,
                    persister: Persister::default(),
                    raft: None,
                    connected: true
                }
            ).take(n).collect(),
            start: std::time::Instant::now()
        };
        
        let tester = Self { 
            config: Arc::new(RwLock::new(config)),
            finished: Default::default()
        };

        for i in 0..n {
            tester.start_one(i, snapshot, false).await?;
        }
        
        Ok(tester)
    }

    async fn begin<D: std::fmt::Display>(&self, desc: D) {
        println!("{}", format!("{desc}...").truecolor(178,225,167));
        self.config.write().await.start = std::time::Instant::now();
    }

    async fn end(&self) -> Result<(), String> {
        self.finished.store(true, Ordering::Release);
        let mut config = self.config.write().await;
        
        let t = config.start.elapsed();
        let nrpc = config.net.rpc_cnt();
        let nbytes = config.net.byte_cnt();
        let ncmd = config.logs.lock().await.max_cmd_idx;
        
        for node in config.nodes.iter_mut() {
            if let Some(raft) = node.raft.take() {
                if let Err(_) = tokio::time::timeout(Duration::from_secs(1), raft.kill()).await {
                    return Err(format!("Raft killing timeout, expect no more than 1sec"));
                }
            }
        }
        config.net.close();

        greet!(" ... Passed --");
        greet!(" {}ms, {} peers, {} rpc, {} bytes, {} cmds", 
            t.as_millis(), config.n, nrpc, nbytes, ncmd);
        Ok(())
    }

    async fn crash_node(&self, node: &mut Node) -> Result<(), String> {
        if let Some(raft) = node.raft.take() {
            node.persister = node.persister.clone().await;
            if let Err(_) = tokio::time::timeout(Duration::from_secs(1), 
                raft.kill()).await
            {
                return Err("Raft kill timeout, expect no more than 1sec".to_string());
            }
        }

        Ok(())
    }

    /// If not restart, only the node with raft.is_none() will be started.
    /// Otherwise, the node will be restarted no matter if there is an alive raft node.
    /// Return a bool to indicate if really a node is restarted.
    async fn start_one(&self, id: usize, snapshot: bool, restart: bool) -> Result<bool, String> {
        let mut config = self.config.write().await;
        let Config {
            net,
            nodes,
            ..
        } = config.deref_mut();
        let node = &mut nodes[id];

        if node.raft.is_some() {
            if !restart {
                return Ok(false);
            }
            self.crash_node(node).await?;
        }

        if let Some(snapshot) = node.persister.snapshot().await {
            self.ingest_snapshot(id, snapshot, None).await;
        }

        let client = net.make_client(id).await;
        let persister = node.persister.clone().await;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let raft = match tokio::time::timeout(
            Duration::from_secs(1),
            Raft::new(client, id, persister, tx)
        ).await {
            Ok(raft) => raft,
            Err(_) => return Err(format!("Raft instantiation timeout, expect no more than 1sec"))
        };
        
        node.raft = Some(raft);
        
        tokio::spawn(Applier {
            id,
            logs: config.logs.clone(),
            apply_ch: rx
        }.run(snapshot));
        Ok(true)
    }

    async fn crash_some(&self, ids: &[usize]) -> Result<(), String> {
        let mut config = self.config.write().await;
        let Config { net, nodes, .. } = &mut config.deref_mut();

        for &id in ids {
            let node = &mut nodes[id];
            if node.connected {
                node.connected = false;
                net.connect(id, false).await;
            }
            self.crash_node(node).await?;
        }
        Ok(())
    }

    async fn crash_one(&self, id: usize) -> Result<(), String> {
        self.crash_some(&[id]).await
    }

    /// Check if only one leader exist for a specific term, 
    /// panics when there are multiple leaders with the same term.
    /// In case of re-election, this check will be performed multiple 
    /// times to find a leader, panics when no leader is found.
    /// Return the id of leader that has the newest term.
    async fn check_one_leader(&self) -> Result<usize, String> {
        for _ in 0..10 {
            let ms = (rand::random::<u64>() % 100) + 450;
            let d = std::time::Duration::from_millis(ms);
            tokio::time::sleep(d).await;

            let mut leader_terms = HashMap::<usize, usize>::new();

            {
                let config = self.config.read().await;
                
                for (id, node) in config.nodes.iter().enumerate() {
                    let (term, is_leader) = match &node.raft {
                        Some(raft) => raft.get_state().await,
                        None => continue
                    };
                    
                    if is_leader {
                        if leader_terms.get(&term).is_some() {
                            return Err(format!("Term {term} has multiple leaders"));
                        }
                        leader_terms.insert(term, id);
                    }
                }
            }

            if let Some(max) = leader_terms.into_iter()
                .max_by_key(|x| x.0) {
                return Ok(max.1);
            }
        }
        Err(format!("Expect one leader, got none"))
    }

    /// Iterate all nodes, ask each one to start a command, 
    /// if success, return its id, the command index and term.
    async fn let_one_start<F>(&self, f: F) -> Option<(usize, (usize, usize))> 
        where F: Fn(usize) -> T
    {
        for (id, raft) in self.config.read().await
            .nodes.iter().enumerate()
            .filter_map(|(id, node)| node.raft.as_ref().map(|raft| (id, raft)))
        {
            let cmd = bincode::serialize(&f(id)).unwrap();
            if let Some(cmd_info) = raft.start(cmd).await {
                return Some((id, cmd_info));
            }
        }
        None
    }

    /// Let a specific node start
    async fn let_it_start<F>(&self, id: usize, f: F) -> Option<(usize, usize)> 
        where F: FnOnce() -> T
    {
        match self.config.read().await
            .nodes.get(id).unwrap()
            .raft.as_ref()
        {
            Some(raft) => {
                let cmd = bincode::serialize(&f()).unwrap();
                raft.start(cmd).await
            },
            None => None
        }
    }

    /// Check if all nodes agree on their terms, 
    /// return the term if agree.
    async fn check_terms(&self) -> Result<usize, String> {
        let mut term = None;
        let config = self.config.read().await;

        for (id, node) in config.nodes.iter().enumerate() {
            if let Some(raft) = &node.raft {
                let (iterm, _) = raft.get_state().await;
                term = match term {
                    Some(term) if term == iterm => Some(term),
                    Some(term) => return Err(format!("Servers {id} 
                            with term {iterm} disagree on term {term}")),
                    None => Some(iterm)
                };
            }
        }
        term.ok_or("Servers return no term".to_string())
    }

    // expect none of the nodes claims to be a leader
    async fn check_no_leader(&self) -> Result<(), String> {
        let config = self.config.read().await;
        for (id, node) in config.nodes.iter().enumerate() {
            if !node.connected || node.raft.is_none() {
                continue;
            }

            let (_, is_leader) = node.raft.as_ref()
                .unwrap().get_state().await;
            if is_leader {
                return Err(format!("Expected no leader among connected servers, 
                    but node {id} claims to be a leader"));
            }
        }
        Ok(())
    }

    /// Wait a command with index to be applied by at least `expected` number of 
    /// servers.
    /// This will wait forever, so it's usually used with timeout function.
    async fn wait_commit(&self, index: usize, cmd: &T, expected: usize) -> Result<usize, String> {
        loop {
            match self.n_committed(index).await? {
                (cnt, Some(cmt_cmd)) => {
                    if cnt >= expected && cmt_cmd == *cmd {
                        break Ok(index);
                    }
                },
                _ => tokio::time::sleep(Duration::from_millis(20)).await
            }
        }
    }

    async fn must_submit(&self, cmd: &T, expected: usize, retry: bool) -> Result<usize, String> {
        let cmd_bin = bincode::serialize(&cmd).unwrap();
        loop {
            // iterate all raft nodes, ask them to start a command.
            // if success, return it.
            let cmd_idx = match {
                let mut index = None;
                // for raft in self.config.read().await.nodes.iter()
                //     .filter(|node| node.connected)
                //     .filter_map(|node| node.raft.as_ref())
                for (_raft_i, raft) in self.config.read().await.nodes.iter().enumerate()
                    .filter(|(_, node)| node.connected)
                    .filter_map(|(i, node)| node.raft.as_ref().map(|nd| (i, nd)))
                {
                    if let Some((cmd_idx, _)) = raft.start(cmd_bin.clone()).await {
                        index = Some(cmd_idx);
                        debug!("Command {cmd} submitted by raft {_raft_i}, index = {cmd_idx}");
                        break;
                    }
                }
                index
            } {
                None => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                },
                Some(idx) => idx
            };
            // debug!("Leader submit command at {cmd_idx}");

            match tokio::time::timeout(Duration::from_secs(2), 
                self.wait_commit(cmd_idx, &cmd, expected)).await
            {
                Ok(res) => break res,
                Err(_) => {
                    if !retry {
                        return Err(format!("One cmd {cmd} failed to reach agreement"));
                    }
                }
            }
        }
    }

    /// Submit a command, ask every node if it is a leader, 
    /// if is, ask it to commit a command.
    /// If the command is successfully committed, return its index.
    async fn submit_cmd(&self, cmd: &T, expected: usize, retry: bool) -> Result<Option<usize>, String> {
        match tokio::time::timeout(Duration::from_secs(10), 
            self.must_submit(cmd, expected, retry)).await
        {
            Ok(res) => Ok(Some(res?)),
            Err(_) => Ok(None)
        }
    }

    /// Submit a command, return Err if not success
    async fn must_submit_cmd(&self, cmd: &T, expected: usize, retry: bool) -> Result<usize, String> {
        match self.submit_cmd(cmd, expected, retry).await? {
            Some(idx) => Ok(idx),
            None => Err(format!("Submit command {cmd} failed"))
        }
    }

    /// Check how many nodes think a command at index is committed.
    /// We assume the applied logs are consistent, so we don't check 
    /// if their values are equal.
    async fn n_committed(&self, idx: usize) -> Result<(usize, Option<T>), String> {
        let mut cnt = 0usize;
        let mut cmd = None;
        let config = self.config.read().await;
        let logs = config.logs.lock().await;
        for (i, log) in logs.logs.iter().enumerate() {
            if let Some(err_msg) = &logs.apply_err[i] {
                return Err(err_msg.clone());
            }

            if let Some(cmd_i) = log.get(idx) {
                match &cmd {
                    None => cmd = Some(cmd_i.clone()),
                    Some(cmd) => {
                        if cmd_i != cmd {
                            return Err(format!(
                                "Command {cmd_i} committed by {i} is inconsistent with others command {cmd}"
                            ));
                        }
                    }
                }
                cnt += 1;
            }
        }
        Ok((cnt, cmd))
    }

    /// Wait a command with `index` to be committed by at least `expect` number
    /// of nodes.
    /// If start_term.is_some(), the waited command must be started at that 
    /// specific term.
    /// Otherwise, as long as the command is committed by specific number of 
    /// servers, the term does not matter.
    async fn wait(&self, index: usize, expect: usize, start_term: Option<usize>) -> Result<Option<T>, String> {
        let mut short_break = Duration::from_millis(10);
        for _ in 0..30 {
            let (n, _) = self.n_committed(index).await?;
            if n >= expect {
                break
            }

            tokio::time::sleep(short_break).await;
            if short_break < Duration::from_secs(1) {
                short_break *= 2;
            }

            if let Some(start_term) = start_term {
                for raft in self.config.read().await.nodes.iter()
                    .filter_map(|node| node.raft.as_ref())
                {
                    let (term, _) = raft.get_state().await;
                    if term > start_term {
                        return Ok(None);
                    }
                }
            }
        }

        let (n, cmd) = self.n_committed(index).await?;
        if n < expect {
            return Err(format!("Only {n} nodes committed command with 
                    index {index}, expected {expect}"));
        }
        Ok(cmd)
    }

    async fn ingest_snapshot(&self, _id: usize, _snapshot: Vec<u8>, _index: Option<usize>) {

    }

    async fn enable(&self, id: usize, enable: bool) {
        let mut config = self.config.write().await;
        config.nodes[id].connected = enable;
        config.net.connect(id, enable).await;
    }

    async fn disconnect(&self, id: usize) {
        self.enable(id, false).await;
    }
    async fn connect(&self, id: usize) {
        self.enable(id, true).await;
    }

    async fn connected(&self, id: usize) -> bool {
        self.config.read().await.nodes[id].connected
    }

    async fn byte_cnt(&self) -> u64 {
        self.config.read().await.net.byte_cnt()
    }
    async fn rpc_cnt(&self) -> u32 {
        self.config.read().await.net.rpc_cnt()
    }

    async fn reliable(&self, value: bool) {
        self.config.read().await.net.set_reliable(value);
    }

    async fn long_reordering(&self, value: bool) {
        self.config.read().await.net.set_long_reordering(value);
    }
}

impl<T> Applier<T> 
    where T: WantedCmd
{
    async fn run(mut self, snap: bool) {
        let mut failed = false;
        while let Some(msg) = self.apply_ch.recv().await {
            if failed { continue; }
            let mut logs = self.logs.lock().await;
            match msg {
                ApplyMsg::Command {index, command} => {
                    failed = Self::check_logs(self.id, index, command, 
                        &mut logs.deref_mut()).await.is_err();
                },
                ApplyMsg::Snapshot {..} if snap => {},
                _ => logs.apply_err[self.id] = Some("Snapshot unexpected".to_string())
            }
        }
    }

    async fn check_logs(id: usize, cmd_idx: usize, cmd: Vec<u8>, logs: &mut Logs<T>) -> Result<(), ()> {
        match Self::cross_check(id, cmd_idx, cmd, &mut logs.logs) {
            Ok(_) => {
                logs.max_cmd_idx = logs.max_cmd_idx.max(cmd_idx);
                Ok(())
            },
            Err(msg) => {
                debug_assert!(logs.apply_err[id].is_none());
                logs.apply_err[id] = Some(msg);
                Err(())
            }
        }
    }

    /// Check applied commands index and term consistency,
    /// if ok, insert this command into logs.
    /// WARN: check_logs assume the Config.lock is hold by the caller.
    fn cross_check(id: usize, cmd_idx: usize, cmd: Vec<u8>,
        logs: &mut Vec<Vec<T>>) -> Result<(), String> {
        let cmd_value = bincode::deserialize_from::<_, T>(&cmd[..])
            .unwrap();

        // the command index can only be equal to the length of the 
        // corresponding log list.
        // if greater, logs applied out of order;
        // if less, the log is applied before, which is not allowed 
        // for a state machine.
        if cmd_idx > logs[id].len() {
            return Err(format!("Server {id} apply out of order, expect {}, got {cmd_idx}", logs[id].len()));
        }
        else if cmd_idx < logs[id].len() {
            return Err(format!("Server {id} has applied the log {cmd_idx} before"));
        }

        // check all logs of other nodes, if the index exist in the logs
        // applied by them. 
        // panics if exist two command values are inconsistent.
        for (i, log) in logs.iter().enumerate() {
            match log.get(cmd_idx) {
                Some(val) if *val != cmd_value => {
                    let err_msg = format!("commit index = {cmd_idx} 
                        server={id} {cmd_value} != server={i} {val}");
                    return Err(err_msg);
                },
                _ => {}
            }
        }

        logs[id].push(cmd_value);
        Ok(())
    }
}

async fn timeout_test<F>(test: F) 
    where F: Future<Output = Result<(), String>>,
{
    let r = match tokio::time::timeout(TEST_TIME_LIMIT, test).await {
        Ok(res) => res,
        Err(_) => Err(format!("Test timeout, expect no more than {} secs", TEST_TIME_LIMIT.as_secs()))
    };
    if let Err(msg) = r {
        panic!("{}", msg.red());
    }
}
