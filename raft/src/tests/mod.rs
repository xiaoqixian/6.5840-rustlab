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

#[cfg(test)]
mod test_3d;

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{
    debug,
    persist::{make_persister, Persister},
    raft::Raft,
    ApplyMsg, UbRx,
};
use colored::Colorize;
use labrpc::network::Network;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinHandle};

/// In async rust, the panic does work as I expected. 
/// It does not stop the whole program, so I let all test functions
/// return this Error, so the tester can stop in time.
/// The PanicErr help record the panic location file name and line number.
#[derive(Clone)]
pub struct PanicErr {
    pub msg: String,
    pub file: &'static str,
    pub line: u32
}

pub type Result<T> = std::result::Result<T, PanicErr>;

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

macro_rules! panic_err {
    ($($args: expr), +) => {
        crate::tests::PanicErr {
            msg: format!($($args), +),
            file: file!(),
            line: line!()
        }
    }
}

#[macro_export]
macro_rules! fatal {
    ($($args: expr), *) => {
        return Err(crate::tests::PanicErr {
            msg: format!($($args),*),
            file: file!(),
            line: line!()
        })
    }
}

const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const TEST_TIME_LIMIT: Duration = Duration::from_secs(120);
const SNAPSHOT_INTERVAL: usize = 10;

struct NodeCore {
    applier_handle: JoinHandle<()>,
    applier_killed: Arc<AtomicBool>,
    raft: Arc<Raft>,
}

struct Node {
    id: usize,
    pub persister: Persister,
    raft_state: Option<Vec<u8>>,
    snapshot: Option<Vec<u8>>,
    core: Option<NodeCore>,
    connected: bool,
}

struct Logs<T> {
    logs: Vec<Vec<T>>,
    apply_err: Vec<Option<PanicErr>>,
    last_applied: Vec<Option<usize>>,
    max_cmd_idx: usize,
}

/// A snapshot contains a list of log entries and a log index 
/// that indicates the last included log index in the snapshot.
#[derive(Serialize)]
struct SnapshotSe<'a, T> {
    lii: usize,
    logs: &'a [T]
}
#[derive(Deserialize)]
struct SnapshotDes<T> {
    // the last included log index
    lii: usize,
    logs: Vec<T>
}

struct Applier<T> {
    id: usize,
    snap: bool,
    raft: Arc<Raft>,
    logs: Arc<Mutex<Logs<T>>>,
    apply_ch: UbRx<ApplyMsg>,
    killed: Arc<AtomicBool>,
}

/// T: the command type
struct Tester<T> {
    n: usize,
    net: Network,
    pub nodes: Vec<Node>,
    logs: Arc<Mutex<Logs<T>>>,
    start: std::time::Instant,
    // config: Arc<RwLock<Config<T>>>,
    finished: Arc<AtomicBool>,
}

trait WantedCmd: Eq + Clone + Serialize + DeserializeOwned + Display + Debug + Send + 'static {}
impl<T> WantedCmd for T where
    T: Eq + Clone + Serialize + DeserializeOwned + Display + Debug + Send + 'static
{
}

impl<T> Logs<T>
where T: WantedCmd 
{
    /// deserialize a snapshot, put all log entries into logs.
    /// if lii.is_some(), the snapshot LII should be checked 
    /// before get ingested.
    fn ingest_snapshot(
        &mut self,
        id: usize,
        snapshot: Vec<u8>,
        lii: Option<usize>,
    ) -> Result<()> {
        let snap: SnapshotDes<T> = match bincode::deserialize_from(&snapshot[..]) {
            Ok(s) => s,
            Err(_) => fatal!("TEST Logs[{id}]: invalid snapshot, unable to decode it")
        };

        if lii.map_or(false, |lii| lii != snap.lii) {
            fatal!("server {id} snapshot LAI does match the real one, expect {}", lii.unwrap());
        }
        
        self.logs[id] = snap.logs;
        self.last_applied[id] = Some(snap.lii);

        Ok(())
    }

}

impl<T> Tester<T>
where
    T: WantedCmd + Sync,
{
    pub async fn new(n: usize, reliable: bool, snapshot: bool) -> Result<Self> {
        let mut tester = Self {
            n,
            net: Network::new(n).reliable(reliable).long_delay(true),
            logs: Arc::new(Mutex::new(Logs {
                logs: vec![Vec::new(); n],
                apply_err: vec![None; n],
                max_cmd_idx: 0,
                last_applied: vec![None; n]
            })),
            nodes: (0..n)
                .into_iter()
                .map(|id| Node {
                    id,
                    persister: Persister::new(),
                    raft_state: None,
                    snapshot: None,
                    core: None,
                    connected: true,
                })
                .collect(),
            start: std::time::Instant::now(),
            finished: Default::default(),
        };

        for i in 0..n {
            tester.start_one(i, snapshot, false).await?;
        }

        Ok(tester)
    }

    async fn begin<D: std::fmt::Display>(&mut self, desc: D) {
        println!("{}", format!("{desc}...").truecolor(178, 225, 167));
        self.start = std::time::Instant::now();
    }

    async fn end(&mut self) -> Result<()> {
        self.finished.store(true, Ordering::Release);

        let t = self.start.elapsed();
        let nrpc = self.net.rpc_cnt();
        let nbytes = self.net.byte_cnt();
        let ncmd = self.logs.lock().await.max_cmd_idx;

        for id in 0..self.n {
            self.crash_one(id).await?;
        }
        self.net.close();

        greet!(" ... Passed --");
        greet!(
            " {}ms, {} peers, {} rpc, {} bytes, {} cmds",
            t.as_millis(),
            self.n,
            nrpc,
            nbytes,
            ncmd
        );
        Ok(())
    }

    async fn crash_node(&mut self, id: usize) -> Result<()> {
        let node = &mut self.nodes[id];
        if let Some(core) = node.core.take() {
            // first close the apply channel, in case the node is taking 
            // a snapshot.
            core.applier_killed.store(true, Ordering::Relaxed);
            if let Err(_) = tokio::time::timeout(Duration::from_secs(1), core.applier_handle).await
            {
                fatal!("Applier long time no return");
            }

            // then replace the original persister with the new created one,
            // this operation makes the old one unavailable.
            let (new_persister, raft_state, snapshot) = make_persister(node.persister.clone())?;
            node.persister = new_persister;
            node.raft_state = raft_state;
            node.snapshot = snapshot;

            debug_assert_eq!(Arc::strong_count(&core.raft), 1);
            match Arc::try_unwrap(core.raft) {
                Ok(raft) => {
                    if let Err(_) = tokio::time::timeout(
                        Duration::from_secs(1), 
                        raft.kill()
                    ).await {
                        fatal!("Raft kill timeout, expect no more than 1sec");
                    }
                },
                Err(_) => fatal!("Try to unwrap core.raft failed")
            }
        }

        Ok(())
    }

    /// If not restart, only the node with raft.is_none() will be started.
    /// Otherwise, the node will be restarted no matter if there is an alive raft node.
    /// Return a bool to indicate if really a node is restarted.
    async fn start_one(
        &mut self,
        id: usize,
        snapshot: bool,
        restart: bool,
    ) -> Result<bool> {
        if self.nodes[id].core.is_some() {
            if !restart {
                return Ok(false);
            }
            self.crash_node(id).await?;
        }

        let node = &mut self.nodes[id];
        
        if let Some(snapshot) = node.snapshot.clone() {
            let mut logs = self.logs.lock().await;
            logs.ingest_snapshot(id, snapshot, None)?;
        }

        let client = self.net.make_client(id).await;
        let persister = node.persister.clone();
        let lai = self.logs.lock().await.last_applied[id].clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let raft = match tokio::time::timeout(
            Duration::from_secs(1),
            Raft::new(
                client,
                id,
                persister,
                tx,
                lai,
                node.raft_state.take(),
                node.snapshot.take()
            ),
        )
        .await
        {
            Ok(raft) => raft,
            Err(_) => {
                fatal!("Raft instantiation timeout, expect no more than 1sec");
            }
        };
        

        let raft = Arc::new(raft);
        let applier_killed = Arc::<AtomicBool>::default();
        let applier_handle = tokio::task::spawn(
            Applier {
                id,
                snap: snapshot,
                raft: raft.clone(),
                logs: self.logs.clone(),
                apply_ch: rx,
                killed: applier_killed.clone(),
            }
            .run(),
        );

        node.core = Some(NodeCore {
            raft,
            applier_handle,
            applier_killed,
        });

        if !node.connected {
            node.connected = true;
            self.net.connect(id, true).await;
        }

        Ok(true)
    }

    async fn crash_one(&mut self, id: usize) -> Result<()> {
        let node = &mut self.nodes[id];
        if node.connected {
            node.connected = false;
            self.net.connect(id, false).await;
        }
        self.crash_node(id).await
    }

    /// Check if only one leader exist for a specific term,
    /// panics when there are multiple leaders with the same term.
    /// In case of re-election, this check will be performed multiple
    /// times to find a leader, panics when no leader is found.
    /// Return the id of leader that has the newest term.
    async fn check_one_leader(&self) -> Result<usize> {
        // check for 10 times.
        for _ in 0..10 {
            let ms = (rand::random::<u64>() % 100) + 450;
            let d = std::time::Duration::from_millis(ms);
            tokio::time::sleep(d).await;

            let mut leader_terms = HashMap::<usize, usize>::new();

            for (id, node) in self.nodes.iter().enumerate() {
                let (term, is_leader) = match &node.core {
                    Some(core) => core.raft.get_state().await,
                    None => continue,
                };

                if is_leader {
                    if leader_terms.get(&term).is_some() {
                        fatal!("Term {term} has multiple leaders");
                    }
                    leader_terms.insert(term, id);
                }
            }

            if let Some(max) = leader_terms.into_iter().max_by_key(|x| x.0) {
                return Ok(max.1);
            }
        }
        fatal!("Expect one leader, got none");
    }

    /// Let a specific node start
    async fn let_it_start(&self, id: usize, cmd: &T) -> Option<(usize, usize)> {
        match self.nodes.get(id).unwrap().core.as_ref() {
            Some(core) => {
                let cmd = bincode::serialize(cmd).unwrap();
                core.raft.start(cmd).await
            }
            None => None,
        }
    }

    /// Check if all nodes agree on their terms,
    /// return the term if agree.
    async fn check_terms(&self) -> Result<usize> {
        let mut term = None;

        for (id, node) in self.nodes.iter().enumerate() {
            if let Some(core) = &node.core {
                let (iterm, _) = core.raft.get_state().await;
                term = match term {
                    Some(term) if term == iterm => Some(term),
                    Some(term) => {
                        fatal!("Server {id} with term {iterm} disagree on term {term}");
                    }
                    None => Some(iterm),
                };
            }
        }
        match term {
            Some(term) => Ok(term),
            None => fatal!("Servers return no term")
        }
    }

    // expect none of the nodes claims to be a leader
    async fn check_no_leader(&self) -> Result<()> {
        for (id, node) in self.nodes.iter().enumerate() {
            if !node.connected || node.core.is_none() {
                continue;
            }

            let (_, is_leader) = node.core.as_ref().unwrap().raft.get_state().await;
            if is_leader {
                fatal!(
                    "Expected no leader among connected servers,\
                    but node {id} claims to be a leader"
                );
            }
        }
        Ok(())
    }

    /// Wait a command with index to be applied by at least `expected` number of
    /// servers.
    /// This will wait forever, so it's usually used with timeout function.
    async fn wait_commit(&self, index: usize, cmd: &T, expected: usize) -> Result<usize> {
        loop {
            match self.n_committed(index).await? {
                (cnt, Some(cmt_cmd)) => {
                    if cnt >= expected && cmt_cmd == *cmd {
                        break Ok(index);
                    }
                }
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    }

    async fn must_submit(&self, cmd: &T, expected: usize, retry: bool) -> Result<usize> {
        let cmd_bin = bincode::serialize(&cmd).unwrap();
        loop {
            // iterate all raft nodes, ask them to start a command.
            // if success, return it.
            let cmd_idx = match {
                let mut index = None;
                // for raft in self.config.read().await.nodes.iter()
                //     .filter(|node| node.connected)
                //     .filter_map(|node| node.raft.as_ref())
                for (_raft_i, raft) in self
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, node)| node.connected)
                    .filter_map(|(i, node)| node.core.as_ref().map(|core| (i, &core.raft)))
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
                }
                Some(idx) => idx,
            };
            // debug!("Leader submit command at {cmd_idx}");

            match tokio::time::timeout(
                Duration::from_secs(2),
                self.wait_commit(cmd_idx, &cmd, expected),
            )
            .await
            {
                Ok(res) => break res,
                Err(_) => {
                    if !retry {
                        fatal!("One cmd {cmd} failed to reach agreement");
                    }
                }
            }
        }
    }

    /// Submit a command, ask every node if it is a leader,
    /// if is, ask it to commit a command.
    /// If the command is successfully committed, return its index.
    async fn submit_cmd(
        &self,
        cmd: &T,
        expected: usize,
        retry: bool,
    ) -> Result<Option<usize>> {
        match tokio::time::timeout(
            Duration::from_secs(10),
            self.must_submit(cmd, expected, retry),
        )
        .await
        {
            Ok(res) => Ok(Some(res?)),
            Err(_) => Ok(None),
        }
    }

    /// Submit a command, return Err if not success
    async fn must_submit_cmd(
        &self,
        cmd: &T,
        expected: usize,
        retry: bool,
    ) -> Result<usize> {
        match self.submit_cmd(cmd, expected, retry).await? {
            Some(idx) => Ok(idx),
            None => fatal!("Submit command {cmd} failed"),
        }
    }

    /// Check how many nodes think a command at index is committed.
    /// We assume the applied logs are consistent, so we don't check
    /// if their values are equal.
    async fn n_committed(&self, idx: usize) -> Result<(usize, Option<T>)> {
        let mut cnt = 0usize;
        let mut cmd = None;
        let logs = self.logs.lock().await;
        for (i, log) in logs.logs.iter().enumerate() {
            if let Some(err) = &logs.apply_err[i] {
                return Err(err.clone());
            }

            if let Some(cmd_i) = log.get(idx) {
                match &cmd {
                    None => cmd = Some(cmd_i.clone()),
                    Some(cmd) => {
                        if cmd_i != cmd {
                            fatal!(
                                "Command {cmd_i} committed by {i} is \
                                inconsistent with others command {cmd}"
                            );
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
    async fn wait(
        &self,
        index: usize,
        expect: usize,
        start_term: Option<usize>,
    ) -> Result<Option<T>> {
        let mut short_break = Duration::from_millis(10);
        for _ in 0..30 {
            let (n, _) = self.n_committed(index).await?;
            if n >= expect {
                break;
            }

            tokio::time::sleep(short_break).await;
            if short_break < Duration::from_secs(1) {
                short_break *= 2;
            }

            if let Some(start_term) = start_term {
                for core in self.nodes.iter().filter_map(|node| node.core.as_ref()) {
                    let (term, _) = core.raft.get_state().await;
                    if term > start_term {
                        return Ok(None);
                    }
                }
            }
        }

        let (n, cmd) = self.n_committed(index).await?;
        if n < expect {
            fatal!(
                "Only {n} nodes committed command with \
                index {index}, expect {expect} nodes committed"
            );
        }
        Ok(cmd)
    }

    async fn enable(&mut self, id: usize, enable: bool) {
        self.nodes[id].connected = enable;
        self.net.connect(id, enable).await;
    }

    async fn disconnect(&mut self, id: usize) {
        self.enable(id, false).await;
    }
    async fn connect(&mut self, id: usize) {
        self.enable(id, true).await;
    }

    async fn connected(&self, id: usize) -> bool {
        self.nodes[id].connected
    }

    async fn byte_cnt(&self) -> u64 {
        self.net.byte_cnt()
    }
    async fn rpc_cnt(&self) -> u32 {
        self.net.rpc_cnt()
    }

    async fn reliable(&self, value: bool) {
        self.net.set_reliable(value);
    }

    async fn long_reordering(&self, value: bool) {
        self.net.set_long_reordering(value);
    }
}

impl<T> Applier<T>
where
    T: WantedCmd + Sync,
{
    async fn check_alive(killed: Arc<AtomicBool>) {
        while !killed.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn run(mut self) {
        loop {
            let msg = match tokio::select! {
                biased;
                msg = self.apply_ch.recv() => msg,
                _ = Self::check_alive(self.killed.clone()) => {
                    debug!("Applier[{}]: no longer alive, close apply channel", self.id);
                    None
                }
            } {
                None => break,
                Some(msg) => msg
            };

            let mut logs = self.logs.lock().await;
            let res = self.process(&mut logs, msg).await;
            if let Err(e) = res {
                logs.apply_err[self.id] = Some(e);
                break;
            }
        }

        self.apply_ch.close();
        while let Some(msg) = self.apply_ch.recv().await {
            let mut logs = self.logs.lock().await;
            let res = self.process(&mut logs, msg).await;
            if let Err(e) = res {
                logs.apply_err[self.id] = Some(e);
                break;
            }
        }
    }

    async fn process(&self, logs: &mut Logs<T>, msg: ApplyMsg) -> Result<()> {
        match msg {
            ApplyMsg::Command { index, command } => {
                debug!("Applier[{}]: applied command {index}", self.id);
                match Self::check_logs(self.id, index, command, logs) {
                    Ok(_) => Self::take_snapshot(&self.raft, &logs, index).await,
                    Err(e) => Err(e)
                }
            }
            ApplyMsg::Snapshot { lii, snapshot } if self.snap => {
                debug!("Applier[{}]: applied snapshot with lii = {lii}", self.id);
                logs.ingest_snapshot(self.id, snapshot, Some(lii))
            }
            _ => {
                Err(panic_err!("Snapshot unexpected"))
            }
        }
    }

    /// Check applied commands index and term consistency,
    /// if ok, insert this command into logs.
    fn check_logs(
        id: usize,
        cmd_idx: usize,
        cmd: Vec<u8>,
        logs: &mut Logs<T>,
    ) -> Result<()> {
        // the command index can only be equal to the length of the
        // corresponding log list.
        // if greater, logs applied out of order;
        // if less, the log is applied before, which is not allowed
        // for a state machine.
        let lap = logs.last_applied[id];
        use std::cmp::Ordering;
        match lap.map(|lap| (lap+1).cmp(&cmd_idx)) {
            Some(Ordering::Less) => fatal!(
                "Server {id} apply out of order, expect {}, got {cmd_idx}",
                lap.unwrap() + 1
            ),
            Some(Ordering::Greater) => fatal!("Server {id} has applied the log {cmd_idx} before."),
            _ => {}
        }

        let cmd_value = bincode::deserialize_from::<_, T>(&cmd[..]).unwrap();

        // check all logs of other nodes, if the index exist in the logs
        // applied by them.
        // panics if exist two command values are inconsistent.
        for (i, log) in logs.logs.iter().enumerate() {
            match log.get(cmd_idx) {
                Some(val) if *val != cmd_value => {
                    fatal!(
                        "commit index = {cmd_idx}, \
                        server={id} {cmd_value} != server={i} {val}"
                    );
                }
                _ => {}
            }
        }

        logs.logs[id].push(cmd_value);
        logs.last_applied[id] = Some(cmd_idx);
        logs.max_cmd_idx = logs.max_cmd_idx.max(cmd_idx);
        Ok(())
    }

    async fn take_snapshot(raft: &Raft, logs: &Logs<T>, index: usize) -> Result<()> {
        // index+1 to avoid making snapshot even when cmd_idx = 0
        if (index + 1) % SNAPSHOT_INTERVAL != 0 {
            return Ok(());
        }

        let snap = SnapshotSe {
            lii: index,
            logs: &logs.logs[raft.me][0..=index]
        };
        let snap_bin = bincode::serialize(&snap).unwrap();
        debug!("Applier [{}]: make a snapshot in range {:?}", raft.me, 0..=index);
        tokio::time::timeout(
            Duration::from_secs(1),
            raft.snapshot(index, snap_bin)
        ).await
        .map_err(|_| panic_err!("Raft {} taking snapshot timeout, \
                expect no more than 1 sec", raft.me))
    }
}

impl<T> std::fmt::Display for Applier<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Applier [{}]", self.id)
    }
}

static PANIC_HOOK_SET: AtomicBool = AtomicBool::new(false);

async fn timeout_test<F>(test: F)
where
    F: Future<Output = Result<()>>,
{
    if !PANIC_HOOK_SET.swap(true, Ordering::Relaxed) {
        std::panic::set_hook(Box::new(|info| {
            if let Some(e) = info.payload().downcast_ref::<PanicErr>() {
                let msg = format!(
                    "thread '{}' panicked at {}:{}\n{}",
                    std::thread::current().name().or(Some("")).unwrap(),
                    e.file,
                    e.line,
                    e.msg
                ).red();
                eprintln!("{msg}");
            }
            else {
                eprintln!("{info}");
            }
        }))
    }

    let r = match tokio::time::timeout(TEST_TIME_LIMIT, test).await {
        Ok(res) => res,
        Err(_) => Err(panic_err!(
            "Test timeout, expect no more than {} secs",
            TEST_TIME_LIMIT.as_secs()
        )),
    };

    if let Err(e) = r {
        std::panic::panic_any(e);
    }
}
