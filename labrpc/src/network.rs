// Date:   Wed May 22 20:54:20 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

// A labrpc network simulation
/*
 * type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool                        // pause a long time on send on disabled connection
	longReordering bool                        // sometimes delay replies a long time
	ends           map[interface{}]*ClientEnd  // ends, by name
	enabled        map[interface{}]bool        // by end name
	servers        map[interface{}]*Server     // servers, by name
	connections    map[interface{}]interface{} // endname -> servername
	endCh          chan reqMsg
	done           chan struct{} // closed when Network is cleaned up
	count          int32         // total RPC count, for statistics
	bytes          int64         // total bytes send, for statistics
}
 */

use std::{
    collections::HashMap, pin::Pin, sync::{
        atomic::{AtomicBool, AtomicU32, Ordering::*}, 
        Arc, RwLock
    }
};

use futures::Future;
use tokio::sync::mpsc as tk_mpsc;
use tokio::sync::oneshot as tk_oneshot;


trait RpcArgs: Send + Sync {}
trait RpcReply: Send + Sync {}
type Sender<T> = tk_mpsc::UnboundedSender<T>;
type Receiver<T> = tk_mpsc::UnboundedReceiver<T>;
type OneshotSender<T> = tk_oneshot::Sender<T>;
type OneshotReceiver<T> = tk_oneshot::Receiver<T>;


type EndId = u32;
type ServerId = u32;

struct NetworkConfig {
    connected: Vec<AtomicBool>
}

struct ClientEnd {
    tx: Arc<Sender<RpcMsg>>, // tx channel to the network
    id: EndId,
}

trait RpcHost: Sync + Send {}

trait RpcFuncTrait: Fn(Arc<dyn RpcHost>, Box<dyn RpcArgs>) -> 
    Pin<Box<dyn Future<Output = Box<dyn RpcReply>> + Send>> + Send + Sync {}

type RpcFunc = &'static dyn RpcFuncTrait;

struct Server {
    hosts: RwLock<HashMap<String, Arc<dyn RpcHost>>>,
    services: RwLock<HashMap<String, RpcFunc>>,
}

struct End {
    tx: Sender<RpcMsg>,
}

struct Network {
    tx: Arc<Sender<RpcMsg>>,
    servers: Arc<RwLock<Vec<Server>>>,
    ends: Arc<RwLock<HashMap<EndId, ServerId>>>,
    end_count: AtomicU32,
}

struct NetworkHandle {
    rx: Receiver<RpcMsg>,
    servers: Arc<RwLock<Vec<Server>>>,
    ends: Arc<RwLock<HashMap<EndId, ServerId>>>,
}

impl Network {
    pub fn new() -> Self {
        let (tx, rx) = tk_mpsc::unbounded_channel::<RpcMsg>();
        let servers = Arc::new(RwLock::new(Vec::new()));
        let ends = Arc::new(RwLock::new(HashMap::new()));

        tokio::spawn(Self::process_req(NetworkHandle {
            rx,
            servers: servers.clone(),
            ends: ends.clone()
        }));

        return Self {
            tx: Arc::new(tx),
            servers,
            ends,
            end_count: AtomicU32::new(0),
        }
    }

    pub fn add_server(&self, server: Server) -> ServerId {
        self.servers.write().unwrap().push(server);
        self.servers.read().unwrap().len() as ServerId
    }

    pub fn make_end(&self) -> ClientEnd {
        let end_id = self.end_count.fetch_add(1, AcqRel);

        ClientEnd {
            tx: self.tx.clone(),
            id: end_id
        }
    }

    // connect a client end to a server.
    #[inline]
    pub fn connect(&self, end_id: EndId, server_id: ServerId) {
        self.ends.write().unwrap().insert(end_id, server_id);
    }

    pub async fn process_req(mut net: NetworkHandle) {
        while let Some(req) = net.rx.recv().await {
            let ends = net.ends.clone();
            let servers = net.servers.clone();

            tokio::spawn(async move {
                let host_func = match ends.read().unwrap().get(&req.from) {
                    None => None,
                    Some(server_id) => {
                        match servers.read().unwrap().get(*server_id as usize) {
                            None => None,
                            Some(server) => server.get_host_func(&req.method)
                        }
                    }
                };

                let _ = req.reply_ch.send(match host_func {
                    None => None,
                    Some((host, func)) => {
                        Some(func(host, req.args).await)
                    }
                });
            });
        }
    }
}

impl Server {
    pub fn get_host(&self, name: &str) -> Option<Arc<dyn RpcHost>> {
        self.hosts.read().unwrap().get(name).map(|h| h.clone())
    }

    pub fn get_host_func(&self, name: &str) -> Option<(Arc<dyn RpcHost>, RpcFunc)> {
        let class = name.split(".").next().unwrap();
        match self.hosts.read().unwrap().get(class) {
            None => None,
            Some(h) => {
                self.services.read().unwrap().get(name)
                    .map(|f| (h.clone(), *f))
            }
        }
    }
}
