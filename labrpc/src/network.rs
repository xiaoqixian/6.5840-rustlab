// Date:   Fri Aug 16 15:36:01 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc}};
use tokio::sync::{mpsc as tk_mpsc, RwLock};

use crate::{
    end::{Client, Server}, err::TIMEOUT, msg::{Msg, RpcReq}, CallResult, UbRx
};

use super::UbTx;

type EndTable = Arc<RwLock<HashMap<u32, EndNode>>>;

#[derive(Clone)]
struct NetworkConfig {
    reliable: Arc<AtomicBool>,
    long_delay: Arc<AtomicBool>
}

#[derive(Clone)]
pub struct Network {
    tx: UbTx<Msg>,
    nodes: EndTable,
    config: NetworkConfig
}

#[derive(Clone)]
struct NetworkDaemon {
    nodes: EndTable,
    config: NetworkConfig
}

struct EndNode {
    server: Server,
    connected: Arc<AtomicBool>
}

impl EndNode {
    #[inline]
    fn connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }
}

impl NetworkConfig {
    #[inline]
    fn reliable(&self) -> bool {
        self.reliable.load(Ordering::Acquire)
    }

    #[inline]
    fn long_delay(&self) -> bool {
        self.long_delay.load(Ordering::Acquire)
    }
}

impl Network {
    pub fn new() -> Self {
        let config = NetworkConfig {
            reliable: Arc::new(AtomicBool::new(true)),
            long_delay: Default::default()
        };

        let (tx, rx) = tk_mpsc::unbounded_channel();

        let nodes: EndTable = Default::default();

        tokio::spawn(NetworkDaemon {
            nodes: nodes.clone(),
            config: config.clone()
        }.run(rx));

        Self {
            tx,
            nodes,
            config
        }
    }

    #[inline]
    pub fn reliable(self, val: bool) -> Self {
        self.set_reliable(val);
        self
    }

    #[inline]
    pub fn set_reliable(&self, val: bool) {
        self.config.reliable.store(val, Ordering::Release);
    }

    pub async fn join(&self, server: Server) -> (u32, UbTx<Msg>) {
        let mut nodes = self.nodes.write().await;
        let id = nodes.len() as u32;
        nodes.insert(id, EndNode {
            server,
            connected: Arc::new(AtomicBool::new(true))
        });
        (id, self.tx.clone())
    }

    pub async fn make_clients(&self) -> Vec<Client> {
        let nodes = self.nodes.read().await;
        nodes.keys()
            .map(|id| Client::new(*id, self.tx.clone()))
            .collect::<Vec<_>>()
    }
}

impl NetworkDaemon {
    async fn run(self, mut rx: UbRx<Msg>) {
        while let Some(msg) = rx.recv().await {
            tokio::spawn(self.clone().process_msg(msg));
        }
    }

    async fn process_msg(self, msg: Msg) {
        let Msg {
            end_id,
            req,
            reply_tx
        } = msg;
        let result = self.dispatch(end_id, req).await;
        reply_tx.send(result).await.unwrap()
    }

    async fn dispatch(&self, end_id: u32, req: RpcReq) -> CallResult {
        let nodes = self.nodes.read().await;

        let node = match nodes.get(&end_id) {
            Some(node) if node.connected() => Some(node),
            _ => None
        };

        if let Some(node) = node {
            let reliable = self.config.reliable();
            if !reliable {
                // give a random short delay.
                let ms = rand::random::<u64>() % 27;
                let d = std::time::Duration::from_millis(ms);
                tokio::time::sleep(d).await;
            }

            if !reliable && rand::random::<u32>() % 1000 < 100 {
                Err(TIMEOUT)
            } else {
                node.server.dispatch(req).await
            }
        } else {
            // simulate no reply and eventual timeout
            let ms = rand::random::<u64>();
            let ms = if self.config.long_delay() {
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
