// Date:   Fri Aug 16 15:36:01 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering}, 
    Arc
};
use tokio::sync::{mpsc as tk_mpsc, RwLock};

use crate::{
    client::Client, 
    err::{DISCONNECTED, PEER_NOT_FOUND, TIMEOUT}, 
    server::Server, CallResult, Idx, Key, Msg, RpcReq, UbRx
};

use super::UbTx;

// const DISPATCH_WAITING: Duration = Duration::from_secs(20);

// The client and server id at index.
#[derive(Clone)]
struct Node {
    key: Key,
    connected: bool,
    server: Server,
}

#[derive(Default)]
struct NetworkCore {
    nodes: Vec<RwLock<Option<Node>>>,
    rpc_cnt: AtomicU32,
    byte_cnt: AtomicU64,
    reliable: AtomicBool,
    long_delay: AtomicBool,
}

pub struct Network {
    n: usize,
    tx: UbTx<Msg>,
    core: Arc<NetworkCore>,
    key_src: AtomicUsize,
}

#[derive(Clone)]
struct NetworkDaemon {
    core: Arc<NetworkCore>
}

impl Network {
    pub fn new(n: usize) -> Self {
        let (tx, rx) = tk_mpsc::unbounded_channel();

        let core = NetworkCore {
            nodes: std::iter::repeat_with(Default::default)
                .take(n).collect(),
            ..Default::default()
        };
        let core = Arc::new(core);

        tokio::spawn(NetworkDaemon {
            core: core.clone()
        }.run(rx));

        Self {
            n,
            tx,
            core,
            key_src: AtomicUsize::default(),
        }
    }

    #[inline]
    pub fn reliable(self, val: bool) -> Self {
        self.set_reliable(val);
        self
    }

    #[inline]
    pub fn set_reliable(&self, val: bool) {
        self.core.reliable.store(val, Ordering::Release);
    }

    #[inline]
    pub fn set_long_delay(&self, val: bool) {
        self.core.long_delay.store(val, Ordering::Release);
    }

    #[inline]
    pub fn long_delay(self, val: bool) -> Self {
        self.set_long_delay(val);
        self
    }

    /// By making a client, the new client will replace the original 
    /// one at `idx`, which expires the key and removes the original
    /// server out of the network.
    /// So this can be used to simulate a restart of a node.
    pub async fn make_client(&mut self, idx: usize) -> Client {
        assert!(idx < self.n, "Invalid index {idx}, 
            the network group size is {}", self.n);

        let key = self.key_src.fetch_add(1, Ordering::AcqRel);
        let server = Server::default();
        *self.core.nodes[idx].write().await = Some(Node {
            key,
            server: server.clone(),
            connected: true
        });
        
        Client::new(key, idx, self.n, server, self.tx.clone())
    }

    #[inline]
    pub fn rpc_cnt(&self) -> u32 {
        self.core.rpc_cnt.load(Ordering::Acquire)
    }

    #[inline]
    pub fn byte_cnt(&self) -> u64 {
        self.core.byte_cnt.load(Ordering::Acquire)
    }
}

impl NetworkDaemon {
    async fn run(self, mut rx: UbRx<Msg>) {
        while let Some(msg) = rx.recv().await {
            self.core.rpc_cnt.fetch_add(1, Ordering::AcqRel);
            let bytes = msg.req.arg.len() as u64;
            self.core.byte_cnt.fetch_add(bytes, Ordering::AcqRel);

            tokio::spawn(self.clone().process_msg(msg));
        }
    }

    async fn process_msg(self, msg: Msg) {
        let Msg {
            key: msg_key,
            from,
            to,
            req,
            reply_tx
        } = msg;
        
        let from_info = match self.core.nodes.get(from) {
            None => None,
            Some(nd) => nd.read().await.as_ref()
                .map(|nd| (nd.connected, nd.key))
        };

        let result = match from_info {
            None => Err(PEER_NOT_FOUND),
            Some((from_conn, from_key)) => 
                match (from_conn, from_key == msg_key) {
                    (true, true) => self.dispatch(to, req).await,
                    _ => Err(DISCONNECTED)
                }
        };

        reply_tx.send(result).unwrap()
    }

    async fn dispatch(&self, to: Idx, req: RpcReq) -> CallResult {
        let node = match self.core.nodes.get(to) {
            None => None,
            Some(nd) => nd.read().await.clone()
        };

        match node {
            Some(node) if node.connected => {
                let reliable = self.core.reliable.load(Ordering::Acquire);

                // give a random short delay.
                if !reliable {
                    let ms = rand::random::<u64>() % 27;
                    let d = std::time::Duration::from_millis(ms);
                    tokio::time::sleep(d).await;
                }

                // randomly discard messages in unreliable network env.
                let discard = !reliable && rand::random::<u32>() % 1000 < 100;

                if discard {
                    Err(TIMEOUT)
                } else {
                    node.server.dispatch(req).await
                }
            },
            _ => {
                // simulate no reply and eventual timeout
                let ms = rand::random::<u64>();
                let long_delay = self.core.long_delay.load(Ordering::Acquire);
                let ms = if long_delay {
                    ms % 7000
                } else {
                    ms % 100
                };

                let d = std::time::Duration::from_millis(ms);
                tokio::time::sleep(d).await;
                Err(TIMEOUT)
            }
        }
    }
}
