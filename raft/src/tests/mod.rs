// Date:   Thu Aug 29 16:27:57 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

#[cfg(test)]
mod test_3a;

#[cfg(test)]
mod test_3b;

use std::{collections::HashMap, fmt::{Debug, Display}, ops::DerefMut, sync::{atomic::{AtomicBool, Ordering}, Arc}, time::Duration};

use labrpc::network::Network;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Mutex, RwLock};
use crate::{ApplyMsg, persist::Persister, raft::Raft, UbRx};
use colored::Colorize;

macro_rules! fatal {
    ($($args: expr),*) => {{
        let msg = format!($($args),*).red();
        panic!("{msg}");
    }}
}

macro_rules! greet {
    ($($args: expr),*) => {{
        let msg = format!($($args),*).truecolor(178,225,167);
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

struct Node {
    last_applied: usize,
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
    max_cmd_idx: usize
}

struct Applier<T> {
    id: usize,
    logs: Arc<Mutex<Logs<T>>>,
    apply_ch: UbRx<ApplyMsg>
}

/// T: the command type
struct Tester<T> {
    config: Arc<RwLock<Config<T>>>,
    time_limit: Duration,
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
    pub async fn new(n: usize, reliable: bool, snapshot: bool, time_limit: Duration) -> Self {
        let config = Config {
            n,
            net: Network::new(n).reliable(reliable).long_delay(true),
            logs: Arc::new(Mutex::new(Logs {
                logs: vec![Vec::new(); n],
                max_cmd_idx: 0
            })),
            nodes: std::iter::repeat_with(
                || Node {
                    last_applied: 0,
                    persister: Persister::default(),
                    raft: None,
                    connected: true
                }
            ).take(n).collect(),
            start: std::time::Instant::now()
        };
        
        let tester = Self { 
            config: Arc::new(RwLock::new(config)),
            time_limit,
            finished: Default::default()
        };

        for i in 0..n {
            tester.start1(i, snapshot).await;
        }
        
        tester
    }

    async fn begin<D: std::fmt::Display>(&self, desc: D) {
        println!("{}", format!("{desc}...").truecolor(178,225,167));
        self.config.write().await.start = std::time::Instant::now();
        
        let time_limit = self.time_limit.clone();
        let finished = self.finished.clone();
        let msg = format!("{desc} unable to finish in {} seconds", 
            time_limit.as_secs());
        tokio::spawn(async move {
            tokio::time::sleep(time_limit).await;

            if !finished.swap(true, Ordering::SeqCst) {
                fatal!("{msg}");
            }
        });
    }

    async fn end(&self) {
        self.finished.store(true, Ordering::Release);
        let mut config = self.config.write().await;
        
        let t = config.start.elapsed();
        let nrpc = config.net.rpc_cnt();
        let nbytes = config.net.byte_cnt();
        let ncmd = config.logs.lock().await.max_cmd_idx;
        
        for node in config.nodes.iter_mut() {
            if let Some(raft) = node.raft.take() {
                if let Err(_) = tokio::time::timeout(Duration::from_secs(1), raft.kill()).await {
                    fatal!("Raft killing timeout, expect no more than 1sec");
                }
            }
        }
        config.net.close();

        greet!(" ... Passed --");
        greet!(" {}ms, {} peers, {} rpc, {} bytes, {} cmds", 
            t.as_millis(), config.n, nrpc, nbytes, ncmd);

    }

    async fn start1(&self, id: usize, snapshot: bool) {
        self.crash1(id).await;

        let mut config = self.config.write().await;
        let Config {
            net,
            nodes,
            ..
        } = config.deref_mut();
        let node = &mut nodes[id as usize];

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
            Err(_) => fatal!("Raft instantiation timeout, expect no more than 1sec")
        };
        
        node.raft = Some(raft);
        
        tokio::spawn(Applier {
            id,
            logs: config.logs.clone(),
            apply_ch: rx
        }.run(snapshot));
    }

    async fn crash1(&self, id: usize) {
        let raft_node = {
            let mut config = self.config.write().await;
            let node = &mut config.nodes[id as usize];

            // as this Persister is hold by the old raft node as well,
            // we need to create a new one so the old one won't affect 
            // this one.
            node.persister = node.persister.clone().await;
            node.raft.take()
        };
        if let Some(raft_node) = raft_node {
            raft_node.kill().await;
        }
    }

    /// Check if only one leader exist for a specific term, 
    /// panics when there are multiple leaders with the same term.
    /// In case of re-election, this check will be performed multiple 
    /// times to find a leader, panics when no leader is found.
    /// Return the id of leader that has the newest term.
    async fn check_one_leader(&self) -> usize {
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
                            fatal!("Term {term} has multiple leaders");
                        }
                        leader_terms.insert(term, id);
                    }
                }
            }

            if let Some(max) = leader_terms.into_iter()
                .max_by_key(|x| x.0) {
                return max.1;
            }
        }
        fatal!("Expect one leader, got none");
    }

    /// Check if all nodes agree on their terms, 
    /// return the term if agree.
    async fn check_terms(&self) -> usize {
        let mut term = None;
        let config = self.config.read().await;

        for (id, node) in config.nodes.iter().enumerate() {
            if let Some(raft) = &node.raft {
                let (iterm, _) = raft.get_state().await;
                term = match term {
                    Some(term) if term == iterm => Some(term),
                    Some(term) => fatal!("Servers {id} with term {iterm} disagree on term {term}"),
                    None => Some(iterm)
                };
            }
        }
        term.expect("Servers return no term")
    }

    // expect none of the nodes claims to be a leader
    async fn check_no_leader(&self) {
        let config = self.config.read().await;
        for (id, node) in config.nodes.iter().enumerate() {
            if !node.connected || node.raft.is_none() {
                continue;
            }

            let (_, is_leader) = node.raft.as_ref()
                .unwrap().get_state().await;
            if is_leader {
                fatal!("Expected no leader among connected servers, 
                    but node {id} claims to be a leader");
            }
        }
    }

    /// Wait a command with index to be applied by at least `expected` number of 
    /// servers.
    /// This will wait forever, so it's usually used with timeout function.
    async fn wait_commit(&self, index: usize, cmd: &T, expected: usize) -> usize {
        loop {
            match self.n_committed(index).await {
                (cnt, Some(cmt_cnd)) => {
                    if cnt >= expected && cmt_cnd == *cmd {
                        break index;
                    }
                },
                _ => tokio::time::sleep(Duration::from_millis(20)).await
            }
        }
    }

    async fn must_submit(&self, cmd: T, expected: usize, retry: bool) -> usize {
        let cmd_bin = bincode::serialize(&cmd).unwrap();
        loop {
            // iterate all raft nodes, ask them to start a command.
            // if success, return it.
            let cmd_idx = match {
                let mut index = None;
                for raft in self.config.read().await.nodes.iter()
                    .filter(|node| node.connected)
                    .filter_map(|node| node.raft.as_ref())
                {
                    if let Some((cmd_idx, _)) = raft.start(cmd_bin.clone()).await {
                        index = Some(cmd_idx);
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
            debug!("Leader submit command at {cmd_idx}");

            match tokio::time::timeout(Duration::from_secs(2), 
                self.wait_commit(cmd_idx, &cmd, expected)).await
            {
                Ok(index) => break index,
                Err(_) => {
                    if !retry {
                        fatal!("One cmd {cmd} failed to reach agreement");
                    }
                }
            }
        }
    }

    /// Commit a command, ask every node if it is a leader, 
    /// if is, ask it to commit a command.
    /// If the command is successfully committed, return its index.
    async fn submit_cmd(&self, cmd: T, expected: usize, retry: bool) -> Option<usize> {
        tokio::time::timeout(Duration::from_secs(10), 
            self.must_submit(cmd, expected, retry)).await.ok()
    }

    /// Check how many nodes think a command at index is committed.
    /// We assume the applied logs are consistent, so we don't check 
    /// if their values are equal.
    async fn n_committed(&self, idx: usize) -> (usize, Option<T>) {
        let mut cnt = 0usize;
        let mut cmd = None;
        for log in self.config.read().await
            .logs.lock().await
            .logs.iter()
        {
            if let Some(cmd_i) = log.get(idx) {
                match &cmd {
                    None => cmd = Some(cmd_i.clone()),
                    Some(cmd) => assert_eq!(cmd, cmd_i)
                }
                cnt += 1;
            }
        }
        (cnt, cmd)
    }

    /// Wait a command with `index` to be committed by at least `target` number
    /// of nodes.
    /// If start_term.is_some(), the waited command is must be started at that 
    /// specific term.
    async fn wait(&self, index: usize, target: usize, start_term: Option<usize>) -> Option<T> {
        let mut short_break = Duration::from_millis(10);
        for _ in 0..30 {
            let (n, _) = self.n_committed(index).await;
            if n >= target {
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
                        return None;
                    }
                }
            }
        }

        let (n, cmd) = self.n_committed(index).await;
        if n < target {
            fatal!("Only {n} nodes committed command with index {index}, expected {target}");
        }
        cmd
    }

    async fn ingest_snapshot(&self, _id: usize, _snapshot: Vec<u8>, _index: Option<usize>) {

    }

    async fn enable(&self, id: usize, enable: bool) {
        let mut config = self.config.write().await;
        config.nodes[id as usize].connected = enable;
        config.net.connect(id, enable).await;
    }

    async fn disconnect(&self, id: usize) {
        self.enable(id, false).await;
    }
    async fn connect(&self, id: usize) {
        self.enable(id, true).await;
    }

    async fn byte_cnt(&self) -> u64 {
        self.config.read().await.net.byte_cnt()
    }
    async fn rpc_cnt(&self) -> u32 {
        self.config.read().await.net.rpc_cnt()
    }
}

impl<T> Applier<T> 
    where T: WantedCmd
{
    async fn run(mut self, snap: bool) {
        while let Some(msg) = self.apply_ch.recv().await {
            match msg {
                ApplyMsg::Command {index, command} => {
                    self.check_logs(self.id, index, command).await;
                },
                ApplyMsg::Snapshot {..} if snap => {},
                _ => fatal!("Snapshot unexpected")
            }
        }
    }

    /// Check applied commands index and term consistency,
    /// if ok, insert this command into logs.
    /// WARN: check_logs assume the Config.lock is hold by the caller.
    async fn check_logs(&self, id: usize, cmd_idx: usize, cmd: Vec<u8>) {
        let cmd_value = bincode::deserialize_from::<_, T>(&cmd[..])
            .unwrap();

        let mut logs_guard = self.logs.lock().await;
        let logs = &mut logs_guard.logs;

        // the command index can only be equal to the length of the 
        // corresponding log list.
        // if greater, logs applied out of order;
        // if less, the log is applied before, which is not allowed 
        // for a state machine.
        if cmd_idx > logs[id].len() {
            fatal!("Server {id} apply out of order {}", cmd_idx);
        }
        else if cmd_idx < logs[id].len() {
            fatal!("Server {id} applied the log {} before", cmd_idx);
        }

        // check all logs of other nodes, if the index exist in the logs
        // applied by them. 
        // panics if exist two command values are inconsistent.
        for (i, log) in logs.iter().enumerate() {
            match log.get(cmd_idx) {
                Some(val) if *val != cmd_value => {
                    fatal!("commit index = {} server={} {} != server={} {}",
                        cmd_idx, 
                        id, cmd_value,
                        i, val
                    );
                },
                _ => {}
            }
        }

        logs[id as usize].push(cmd_value);
        logs_guard.max_cmd_idx = usize::max(logs_guard.max_cmd_idx, cmd_idx);
    }
}
