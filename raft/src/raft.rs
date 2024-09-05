// Date:   Thu Aug 29 11:07:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::Arc, time::Duration};

use labrpc::client::Client;
use tokio::sync::Mutex;

use crate::{
    event::{EvQueue, Event}, follower::Follower, msg::ApplyMsg, persist::Persister, Role, UbRx, UbTx
};

pub struct RaftCore {
    pub(crate) me: u32,
    pub(crate) rpc_client: Client,
    pub(crate) persister: Persister,
    pub(crate) apply_ch: UbTx<ApplyMsg>,
    pub(crate) term: usize,
    pub(crate) ev_q: EvQueue
}

// The Raft object to implement a single raft node.
#[derive(Clone)]
pub struct Raft {
    ev_q: EvQueue
}

struct EventProcessor {
    ev_ch: UbRx<Event>,
    core: RaftCore
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
    pub fn new(rpc_client: Client, me: u32, persister: Persister, 
        apply_ch: UbTx<ApplyMsg>) -> Self {

        let (ev_ch_tx, ev_ch_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            ev_q: EvQueue::new(ev_ch_tx)
        }
    }

    /// Get the state of this server, 
    /// return the server's term and if itself believes it's a leader.
    pub async fn get_state(&self) -> (usize, bool) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut ev = Event::GetState(tx);
        loop {
            ev = match self.ev_q.put(ev).await {
                Ok(_) => break,
                Err(ev) => ev
            };
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    pub async fn read_persist(&mut self, bytes: &[u8]) {
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
    pub async fn snapshot(&self, index: usize, snapshot: Vec<u8>) {

    }

    /// Start a Command
    /// If the server believes it's a leader, it should return a 
    /// Some((A, B)), where A is the index that the command will appear
    /// at if it's ever committed, B is the current term.
    /// If it does not, it should just return None.
    pub async fn start(&mut self, command: Vec<u8>) -> Option<(usize, usize)> {
        None
    }

    /// Kill the server.
    pub async fn kill(&self) {}
}
