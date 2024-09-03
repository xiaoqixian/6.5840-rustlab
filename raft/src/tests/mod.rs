// Date:   Thu Aug 29 16:27:57 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

#[cfg(test)]
mod test_3a;

use std::{collections::HashMap, ops::DerefMut, sync::Arc};

use labrpc::network::Network;
use tokio::sync::{Mutex, RwLock};
use crate::{msg::{ApplyMsg, Command}, persist::Persister, raft::Raft, Rx};

struct Node {
    last_applied: usize,
    persister: Persister,
    raft: Option<Raft>,
    connected: bool
}

struct Config {
    n: usize,
    net: Network,
    bytes: usize,
    nodes: Vec<Node>,
    logs: Arc<Mutex<Logs>>,
    
    start: std::time::Instant
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

struct Tester(Arc<RwLock<Config>>);

impl Tester {
    pub async fn new(n: usize, reliable: bool, snapshot: bool) -> Self {
        let config = Config {
            n,
            bytes: 0,
            net: Network::new().reliable(reliable).long_delay(true),
            logs: Arc::new(Mutex::new(Logs {
                logs: vec![Vec::new(); n],
                max_cmd_indx: 0
            })),
            nodes: (0..n).into_iter()
                .map(|_| Node {
                    last_applied: 0,
                    persister: Persister::default(),
                    raft: None,
                    connected: true
                })
                .collect(),
            start: std::time::Instant::now()
        };
        
        let tester = Self(Arc::new(RwLock::new(config)));
        for i in 0..n as u32 {
            tester.start1(i, snapshot).await;
        }
        tester
    }

    async fn begin<T: std::fmt::Display>(&self, desc: T) {
        println!("{desc}...");
        self.0.write().await.start = std::time::Instant::now();
    }

    async fn end(&self) {
        let config = self.0.read().await;
        
        let t = config.start.elapsed();
        let nrpc = config.net.rpc_cnt();
        let nbytes = config.net.byte_cnt();
        let ncmd = config.logs.lock().await.max_cmd_indx;
        
        println!(" ... Passed --");
        println!(" {}ms, {} peers, {} rpc, {} bytes, {} cmds", 
            t.as_millis(), config.n, nrpc, nbytes, ncmd);
    }

    async fn start1(&self, id: u32, snapshot: bool) {
        self.crash1(id).await;

        let mut config = self.0.write().await;
        let Config {
            net,
            nodes,
            ..
        } = config.deref_mut();
        let node = &mut nodes[id as usize];

        if let Some(snapshot) = node.persister.snapshot().await {
            self.ingest_snapshot(id, snapshot, None).await;
        }

        let client = net.join_one().await;
        let persister = node.persister.clone().await;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let raft = Raft::new(client, id, Some(persister), tx);
        
        node.raft = Some(raft);
        
        tokio::spawn(Applier {
            id,
            logs: config.logs.clone(),
            apply_ch: rx
        }.run(snapshot));
    }

    async fn crash1(&self, id: u32) {
        let raft_node = {
            let mut config = self.0.write().await;
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
    async fn check_one_leader(&self) -> u32 {
        for _ in 0..10 {
            let ms = (rand::random::<u64>() % 100) + 450;
            let d = std::time::Duration::from_millis(ms);
            tokio::time::sleep(d).await;

            let mut leader_terms = HashMap::<usize, u32>::new();

            {
                let config = self.0.read().await;
                
                for (id, node) in config.nodes.iter().enumerate() {
                    let (term, is_leader) = match &node.raft {
                        Some(raft) => raft.get_state().await,
                        None => continue
                    };
                    
                    if is_leader {
                        if leader_terms.get(&term).is_some() {
                            panic!("Term {term} has multiple leaders");
                        }
                        leader_terms.insert(term, id as u32);
                    }
                }
            }

            if let Some(max) = leader_terms.into_iter()
                .max_by_key(|x| x.0) {
                return max.1;
            }
        }
        panic!("Expect one leader, got none");
    }

    /// Check if all nodes agree on their terms, 
    /// return the term if agree.
    async fn check_terms(&self) -> usize {
        let mut term = None;
        let config = self.0.read().await;

        for (id, node) in config.nodes.iter().enumerate() {
            if let Some(raft) = &node.raft {
                let (iterm, _) = raft.get_state().await;
                term = match term {
                    Some(term) if term == iterm => Some(term),
                    Some(term) => panic!("Servers {id} with term {iterm} disagree on term {term}"),
                    None => Some(iterm)
                };
            }
        }
        term.expect("Servers return no term")
    }

    // expect none of the nodes claims to be a leader
    async fn check_no_leader(&self) {
        let config = self.0.read().await;
        for (id, node) in config.nodes.iter().enumerate() {
            if !node.connected || node.raft.is_none() {
                continue;
            }

            let (_, is_leader) = node.raft.as_ref()
                .unwrap().get_state().await;
            if is_leader {
                panic!("Expected no leader among connected servers, 
                    but {id} claims to be a leader");
            }
        }
    }

    async fn ingest_snapshot(&self, id: u32, snapshot: Vec<u8>, index: Option<usize>) {

    }

    async fn enable(&self, id: u32, enable: bool) {
        let mut config = self.0.write().await;
        config.nodes[id as usize].connected = enable;
        config.net.enable(id, enable).await;
    }

    #[inline]
    async fn disconnect(&self, id: u32) {
        self.enable(id, false).await;
    }
    #[inline]
    async fn connect(&self, id: u32) {
        self.enable(id, true).await;
    }
}

impl Applier {
    async fn run(mut self, snap: bool) {
        while let Some(msg) = self.apply_ch.recv().await {
            match msg {
                ApplyMsg::Command(cmd) => {
                    
                },
                ApplyMsg::Snapshot(snapshot) if snap => {},
                _ => panic!("Snapshot unexpected")
            }
        }
    }

    /// Check applied commands index and term consistency,
    /// if ok, insert this command into logs.
    /// WARN: check_logs assume the Config.lock is hold by the caller.
    async fn check_logs(&self, id: u32, cmd: Command) {
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
