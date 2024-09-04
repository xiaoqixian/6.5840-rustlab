// Date:   Fri Aug 16 15:36:01 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{collections::HashMap, sync::{atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering}, Arc}};
use tokio::sync::{mpsc as tk_mpsc, RwLock};

use crate::{
    err::TIMEOUT, 
    msg::{Msg, RpcReq}, 
    Service, CallResult, UbRx,
    client::{Client, ClientEnd},
    server::Server
};

use super::UbTx;

pub(crate) type Peers = Arc<RwLock<HashMap<usize, ClientEnd>>>;
pub(crate) type ServiceContainer = Arc<RwLock<HashMap<String, Box<dyn Service>>>>;

type ServerTable = Arc<RwLock<Vec<Option<ServerNode>>>>;

#[derive(Clone)]
struct NetworkConfig {
    reliable: Arc<AtomicBool>,
    long_delay: Arc<AtomicBool>
}

#[derive(Clone)]
pub struct Network {
    tx: UbTx<Msg>,
    config: NetworkConfig,
    nodes: ServerTable,
    peers: Peers,
    rpc_cnt: Arc<AtomicU32>,
    byte_cnt: Arc<AtomicU64>,
    connected: HashMap<usize, Arc<AtomicBool>>
}

#[derive(Clone)]
struct NetworkDaemon {
    nodes: ServerTable,
    config: NetworkConfig,
    rpc_cnt: Arc<AtomicU32>,
    byte_cnt: Arc<AtomicU64>
}

struct ServerNode {
    server: Server,
    connected: Arc<AtomicBool>
}

impl ServerNode {
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

        let nodes = ServerTable::default();
        let rpc_cnt: Arc<AtomicU32> = Default::default();
        let byte_cnt: Arc<AtomicU64> = Default::default();

        tokio::spawn(NetworkDaemon {
            nodes: nodes.clone(),
            config: config.clone(),
            rpc_cnt: rpc_cnt.clone(),
            byte_cnt: byte_cnt.clone()
        }.run(rx));

        Self {
            tx,
            config,
            nodes,
            peers: Default::default(),
            rpc_cnt,
            byte_cnt,
            connected: Default::default()
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

    #[inline]
    pub fn set_long_delay(&self, val: bool) {
        self.config.long_delay.store(val, Ordering::Release);
    }

    #[inline]
    pub fn long_delay(self, val: bool) -> Self {
        self.set_long_delay(val);
        self
    }

    // join a old or new server, if id.is_none(), 
    // then it's a new server.
    async fn join(&mut self, id: Option<usize>) -> Client {
        let services = ServiceContainer::default();

        // let the nodes lock early released.
        let id = {
            let mut nodes = self.nodes.write().await;
            let id = match id {
                Some(id) => id,
                None => {
                    nodes.push(None);
                    nodes.len() - 1
                }
            };
            assert!(id < nodes.len());

            nodes[id] = Some(ServerNode {
                server: Server::new(services.clone()),
                connected: Arc::new(AtomicBool::new(true))
            });
            id
        };

        let mut peers = self.peers.write().await;
        peers.insert(id, ClientEnd::new(id, self.tx.clone()));

        let connected = Arc::new(AtomicBool::new(true));
        self.connected.insert(id, connected.clone());
        Client::new(id, self.peers.clone(), services, connected)
    }

    #[inline]
    pub async fn join_one(&mut self) -> Client {
        self.join(None).await
    }

    #[inline]
    pub async fn join_at(&mut self, id: usize) -> Client {
        self.join(Some(id)).await
    }
    
    /// Delete a server from the cluster
    pub async fn delete_server(&mut self, id: usize) {
        {
            let mut peers = self.peers.write().await;
            if let None = peers.remove(&id) {
                panic!("Server {id} does not exist in the network");
            }
        }

        let mut servers = self.nodes.write().await;
        let ret = servers.get_mut(id).take();
        assert!(ret.is_some());
    }

    pub async fn enable(&mut self, id: usize, enable: bool) {
        let servers = self.nodes.write().await;
        if let Some(Some(server)) = servers.get(id) {
            server.connected.store(enable, Ordering::Release);
        }

        // disable client
        let conn = self.connected.get(&id)
            .expect(&format!("Expect client {id} exist"));
        conn.store(enable, Ordering::Release);
    }

    /// Isolate a server, disconnect it from all other nodes.
    /// But the server is still running, so it can be reconnected.
    #[inline]
    pub async fn disconnect(&mut self, id: usize) {
        self.enable(id, false).await;
    }

    #[inline]
    pub async fn connect(&mut self, id: usize) {
        self.enable(id, true).await;
    }

    #[inline]
    pub fn rpc_cnt(&self) -> u32 {
        self.rpc_cnt.load(Ordering::Acquire)
    }

    #[inline]
    pub fn byte_cnt(&self) -> u64 {
        self.byte_cnt.load(Ordering::Acquire)
    }
}

impl NetworkDaemon {
    async fn run(self, mut rx: UbRx<Msg>) {
        while let Some(msg) = rx.recv().await {
            self.rpc_cnt.fetch_add(1, Ordering::AcqRel);
            let bytes = msg.req.arg.len() as u64;
            self.byte_cnt.fetch_add(bytes, Ordering::AcqRel);

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
        reply_tx.send(result).unwrap()
    }

    async fn dispatch(&self, end_id: usize, req: RpcReq) -> CallResult {
        let nodes = self.nodes.read().await;

        let node = match nodes.get(end_id) {
            Some(Some(node)) if node.connected() => Some(node),
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
