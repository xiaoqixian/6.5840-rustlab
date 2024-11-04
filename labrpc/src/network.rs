// Date:   Fri Aug 16 15:36:01 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering}, 
    Arc
}, time::Duration};
use tokio::sync::{mpsc as tk_mpsc, RwLock};

use crate::{
    client::Client, err::{DISCONNECTED, PEER_NOT_FOUND, TIMEOUT}, server::Server, CallResult, Idx, Key, Msg, RpcReq, UbRx
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
    long_reordering: AtomicBool,
    closed: AtomicBool
}

pub struct Network {
    n: usize,
    tx: UbTx<Msg>,
    core: Arc<NetworkCore>,
    key_src: AtomicUsize,
}

impl Network {
    pub fn new(n: usize) -> Self {
        let (tx, rx) = tk_mpsc::unbounded_channel();

        let core = NetworkCore {
            nodes: std::iter::repeat_with(Default::default)
                .take(n).collect(),
            reliable: AtomicBool::new(true),
            ..Default::default()
        };
        let core = Arc::new(core);

        tokio::spawn(core.clone().run(rx));

        Self {
            n,
            tx,
            core,
            key_src: AtomicUsize::default(),
        }
    }

    pub fn close(&self) {
        self.core.closed.store(true, Ordering::Relaxed);
    }

    #[inline]
    pub fn reliable(self, val: bool) -> Self {
        self.set_reliable(val);
        self
    }

    #[inline]
    pub fn set_reliable(&self, val: bool) {
        self.core.reliable.store(val, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_long_delay(&self, val: bool) {
        self.core.long_delay.store(val, Ordering::Relaxed);
    }

    #[inline]
    pub fn long_delay(self, val: bool) -> Self {
        self.set_long_delay(val);
        self
    }

    #[inline]
    pub fn set_long_reordering(&self, val: bool) {
        self.core.long_reordering.store(val, Ordering::Relaxed);
    }

    #[inline]
    pub fn long_reordering(self, val: bool) -> Self {
        self.set_long_reordering(val);
        self
    }

    /// By making a client, the new client will replace the original 
    /// one at `idx`, which expires the key and removes the original
    /// server out of the network.
    /// So this can be used to simulate a restart of a node.
    pub async fn make_client(&mut self, idx: usize) -> Client {
        debug_assert!(!self.core.closed.load(Ordering::Relaxed),
            "make_client: the network is closed");
        debug_assert!(idx < self.n, "Invalid index {idx}, 
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
        self.core.rpc_cnt.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn byte_cnt(&self) -> u64 {
        self.core.byte_cnt.load(Ordering::Relaxed)
    }

    pub async fn connect(&self, idx: usize, conn: bool) {
        assert!(idx < self.n, "Invalid index {idx}, 
            the network group size is {}", self.n);
        let mut node = self.core.nodes[idx].write().await;
        if let Some(node) = node.as_mut() {
            node.connected = conn;
        }
    }
}

impl NetworkCore {
    async fn run(self: Arc<Self>, mut rx: UbRx<Msg>) {
        while let Some(msg) = rx.recv().await {
            self.rpc_cnt.fetch_add(1, Ordering::Relaxed);
            let bytes = msg.req.arg.len() as u64;
            self.byte_cnt.fetch_add(bytes, Ordering::Relaxed);

            tokio::spawn(self.clone().process_msg(msg));

            if self.closed.load(Ordering::Relaxed) {
                rx.close();
                break;
            }
        }
    }

    async fn process_msg(self: Arc<Self>, msg: Msg) {
        let Msg {
            key: msg_key,
            from,
            to,
            req,
            reply_tx
        } = msg;
        
        let from_info = match self.nodes.get(from) {
            None => None,
            Some(nd) => nd.read().await.as_ref()
                .map(|nd| (nd.connected, nd.key == msg_key))
        };

        let result = match from_info {
            Some((true, true)) => self.clone().dispatch(to, req).await,
            Some(_) => Err(DISCONNECTED),
            None => Err(PEER_NOT_FOUND)
        };

        if let Ok(r) = &result {
            self.byte_cnt.fetch_add(r.len() as u64, Ordering::Relaxed);
        }

        let _ = reply_tx.send(result);
    }

    async fn dispatch(self: Arc<Self>, to: Idx, req: RpcReq) -> CallResult {
        let node = match self.nodes.get(to) {
            None => None,
            Some(nd) => nd.read().await.clone()
        };
        use rand::Rng;

        match node {
            Some(node) if node.connected => {
                let reliable = self.reliable.load(Ordering::Relaxed);

                // give a random short delay.
                if !reliable {
                    let ms = rand::random::<u64>() % 27;
                    let d = Duration::from_millis(ms);
                    tokio::time::sleep(d).await;
                }

                // randomly discard messages in unreliable network env.
                let discard = !reliable && rand::thread_rng().gen_bool(0.1);
                let reorder = self.long_reordering.load(Ordering::Relaxed) &&
                    rand::thread_rng().gen_bool(2f64 / 3f64);

                if discard {
                    Err(TIMEOUT)
                } else {
                    if reorder {
                        let ms = 200u64 + rand::thread_rng().gen_range(1..=2000);
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                    }
                    node.server.dispatch(req).await
                }
            },
            _ => {
                // simulate no reply and eventual timeout
                let ms = rand::random::<u64>();
                let long_delay = self.long_delay.load(Ordering::Relaxed);
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
