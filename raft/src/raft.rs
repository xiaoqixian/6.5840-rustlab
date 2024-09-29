// Date:   Thu Aug 29 11:07:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc}, time::Duration};

use labrpc::client::Client;

use crate::{
    event::{EvQueue, Event}, follower::Follower, info, log::Logs, msg::ApplyMsg, persist::Persister, role::Role, service::RpcService, UbRx, UbTx
};

pub(crate) struct RaftCore {
    pub me: usize,
    pub dead: Arc<AtomicBool>,
    pub rpc_client: Client,
    pub persister: Persister,
    pub apply_ch: UbTx<ApplyMsg>,
    pub term: usize,
    // ev_q is shared
    pub ev_q: Arc<EvQueue>,
    pub vote_for: Option<usize>
}

#[derive(Clone)]
pub(crate) struct RaftHandle {
    pub ev_q: Arc<EvQueue>,
    dead: Arc<AtomicBool>
}

// The Raft object to implement a single raft node.
pub struct Raft {
    ev_q: Arc<EvQueue>,
    dead: Arc<AtomicBool>
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
        let dead = Arc::<AtomicBool>::default();

        let core = RaftCore {
            me,
            dead: dead.clone(),
            rpc_client,
            persister,
            apply_ch,
            term: 0,
            ev_q: ev_q.clone(),
            vote_for: None
        };

        let handle = RaftHandle {
            ev_q: ev_q.clone(),
            dead: dead.clone()
        };

        let logs = Logs::new();

        core.rpc_client.add_service("RpcService".to_string(), 
            Box::new(RpcService::new(handle))).await;

        let flw = Follower::new(core, logs).await;
        tokio::spawn(Self::process_ev(Role::Follower(flw), ev_ch_rx));

        info!("Raft instance {me} started.");

        Self {
            ev_q,
            dead
        }
    }

    /// Get the state of this server, 
    /// return the server's term and if itself believes it's a leader.
    pub async fn get_state(&self) -> (usize, bool) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::GetState(tx);
        self.ev_q.just_put(ev).await;
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
    pub async fn start(&self, command: Vec<u8>) -> Option<(usize, usize)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::StartCmd {
            cmd: command,
            reply_tx: tx
        };
        self.ev_q.just_put(ev).await;
        rx.await.unwrap()
    }

    /// Kill the server.
    pub async fn kill(&self) {
        self.dead.store(true, Ordering::Release);
    }

    async fn process_ev(mut role: Role, mut ev_ch_rx: UbRx<Event>) {
        while let Some(ev) = ev_ch_rx.recv().await {
            role.process(ev).await;
        }
    }
}

impl RaftHandle {
    #[inline]
    pub fn dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }
}

impl std::fmt::Display for RaftCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Raft {},term={}]", self.me, self.term)
    }
}
