// Date:   Thu Aug 29 11:07:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc}, time::Duration};

use labrpc::client::Client;
use tokio::sync::Mutex;

use crate::{
    event::{EvQueue, Event}, follower::Follower, info, log::{Logs, LogsImpl}, msg::ApplyMsg, persist::Persister, role::Role, service::RpcService, UbRx, UbTx
};

pub(crate) struct RaftCoreImpl {
    pub me: usize,
    pub dead: AtomicBool,
    pub rpc_client: Client,
    pub persister: Persister,
    pub apply_ch: UbTx<ApplyMsg>,
    pub term: AtomicUsize,
    // ev_q is shared
    pub ev_q: Arc<EvQueue>,
    pub vote_for: Mutex<Option<usize>>
}

pub(crate) type RaftCore = Arc<RaftCoreImpl>;

// The Raft object to implement a single raft node.
pub struct Raft {
    core: RaftCore,
}

/// Raft implementation.
/// Most API are marked as async functions, and they will called
/// in async way. So you should not modify the asyncness of a function.
/// If you don't need the function to async, just don't call any other 
/// async function in the function body, they will just be like a normal 
/// function.
/// Even though, as this lab is designed in async way. You may want to 
/// use as much async functions as you can, so your code can run in the 
/// best way. For example, use the async waitable lock to replace the 
/// std lock.
impl Raft {
    /// To create a Raft server.
    ///
    /// You can dial any other peers with client unicast or multicast method.
    ///
    /// `me` is the id of this Raft server, each Raft server owns an unique 
    /// id.
    ///
    /// `persister` is a place for this server to save its persistent state 
    /// and also holds the most recent saved state, if any.
    ///
    /// `apply_ch` is a channel on which the tester or service expects Raft 
    /// to send ApplyMsg message.
    pub async fn new(rpc_client: Client, me: usize, persister: Persister, 
        apply_ch: UbTx<ApplyMsg>) -> Self {

        let (ev_ch_tx, ev_ch_rx) = tokio::sync::mpsc::unbounded_channel();
        let ev_q = Arc::new(EvQueue::new(ev_ch_tx, me));

        let core = RaftCoreImpl {
            me,
            dead: AtomicBool::default(),
            rpc_client,
            persister,
            apply_ch,
            term: Default::default(),
            ev_q,
            vote_for: Default::default()
        };
        let core = Arc::new(core);
        let logs = LogsImpl::new(core.clone());

        core.rpc_client.add_service("RpcService".to_string(), 
            Box::new(RpcService::new(core.clone()))).await;

        let flw = Follower::new(core.clone(), logs);
        tokio::spawn(Self::process_ev(Role::Follower(flw), ev_ch_rx));

        info!("Raft instance {me} started.");

        Self {
            core,
        }
    }

    /// Get the state of this server, 
    /// return the server's term and if itself believes it's a leader.
    pub async fn get_state(&self) -> (usize, bool) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::GetState(tx);
        self.core.ev_q.just_put(ev).await;
        rx.await.unwrap()
    }

    /// Persist essential data with persister, the data will be used 
    /// on the next restart.
    pub async fn persist(&self) {
        // Your code here (3C).
        // Example: 
        // Create a serilizable struct Data to store necessary data
        // let data = Data::new(self);
        // let bytes = bincode::serilize(&data).unwrap();
        // self.persister.save(bytes).await;
    }

    /// Read data from a byte buffer to restore Raft server state.
    pub async fn read_persist(&mut self, _bytes: &[u8]) {
        // read Data from bytes
        // Example:
        // let data: Data = bincode::deserilize_from(&bytes).unwrap();
        // self.xxx = data.xxx;
        // self.yyy = data.yyy;
        // ...
    }

    /// The service says it has created a snapshot that has
    /// all info up to and including index. this means the
    /// service no longer needs the log through (and including)
    /// that index. Raft should now trim its log as much as possible.
    pub async fn snapshot(&self, _index: usize, _snapshot: Vec<u8>) {

    }

    /// Start a Command
    /// If the server believes it's a leader, it should return a 
    /// Some((A, B)), where A is the index that the command will appear
    /// at if it's ever committed, B is the current term.
    /// If it does not, it should just return None.
    pub async fn start(&mut self, _command: Vec<u8>) -> Option<(usize, usize)> {
        None
    }

    /// Kill the server.
    pub async fn kill(&self) {
        self.core.dead.store(true, Ordering::Release);
    }

    async fn process_ev(mut role: Role, mut ev_ch_rx: UbRx<Event>) {
        while let Some(ev) = ev_ch_rx.recv().await {
            role.process(ev).await;
        }
    }
}

impl std::fmt::Display for Raft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft {}", self.core.me)
    }
}
impl std::fmt::Display for RaftCoreImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft {}", self.me)
    }
}

impl RaftCoreImpl {
    #[inline]
    pub fn dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    #[inline]
    pub fn term(&self) -> usize {
        self.term.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_term(&self, term: usize) {
        self.term.store(term, Ordering::Relaxed);
    }
}

