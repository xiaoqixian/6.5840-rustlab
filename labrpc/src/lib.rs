// Date:   Tue May 14 22:19:34 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::collections::HashMap;
use std::sync::atomic::{ AtomicBool, Ordering::* };
use std::sync::Arc;
use std::sync::RwLock;

use tokio::sync::mpsc as tk_mpsc;
use tokio::sync::oneshot as tk_oneshot;
use async_trait::async_trait;
use serde::{Serialize, Deserialize, de::DeserializeOwned};

mod map;

use map::MapRx;

type Sender<T> = tk_mpsc::UnboundedSender<T>;
type Receiver<T> = tk_mpsc::UnboundedReceiver<T>;
type OneshotSender<T> = tk_oneshot::Sender<T>;
type OneshotReceiver<T> = tk_oneshot::Receiver<T>;

type ServerId = usize;

trait RpcArgs: Serialize + DeserializeOwned {}
trait RpcReply: Serialize + DeserializeOwned {}

// R represents the reply value type
#[derive(Serialize, Deserialize)]
enum Reply<R> {
    // Service Not Found
    NotFound,
    Success(R)
}

impl<R: DeserializeOwned> From<Vec<u8>> for Reply<R> {
    fn from(value: Vec<u8>) -> Self {
        match bincode::deserialize::<Reply<Vec<u8>>>(&value).unwrap() {
            Reply::NotFound => Reply::NotFound,
            Reply::Success(bytes) => Reply::Success(
                bincode::deserialize::<R>(&bytes).unwrap()
            )
        }
    }
}

#[derive(Clone)]
enum MsgType {
    Unicast(ServerId),
    Broadcast
}

#[derive(Clone)]
struct RpcMsg {
    msg_type: MsgType,
    from: ServerId,
    method: String,
    args: Vec<u8>,
    reply_ch: Arc<Sender<Vec<u8>>>,
}

#[async_trait]
trait Service: Send + Sync {
    async fn call(&self, args: Vec<u8>) -> Vec<u8>;
}

#[derive(Clone)]
struct ServiceTable(
    Arc<RwLock<HashMap<String, Arc<dyn Service>>>>
);

impl ServiceTable {
    fn new() -> Self {
        Self (
            Arc::new(RwLock::new(HashMap::new()))
        )
    }

    fn add_service(&self, name: &str, service: Box<dyn Service>) {
        self.0.write().unwrap().insert(String::from(name), Arc::from(service));
    }

    fn get_service(&self, name: &str) -> Option<Arc<dyn Service>> {
        self.0.read().unwrap().get(name)
            .map(|v| v.clone())
    }
}

struct Node {
    tx: Sender<RpcMsg>,
    enabled: AtomicBool,
}

#[derive(Clone)]
struct Server {
    ct_tx: Arc<Sender<RpcMsg>>,
    services: ServiceTable,
}

struct Client {
    id: ServerId,
    ct_tx: Arc<Sender<RpcMsg>>,
    services: ServiceTable,
}

struct Network {
    servers: Arc<RwLock<Vec<Node>>>,
    tx: Arc<Sender<RpcMsg>>,
}
#[derive(Clone)]
struct NetworkCore {
    servers: Arc<RwLock<Vec<Node>>>
}

impl NetworkCore {
    async fn serve(self, mut rx: Receiver<RpcMsg>) {
        while let Some(msg) = rx.recv().await {
            tokio::spawn(
                self.clone().process_msg(msg)
            );
        }
    }

    async fn process_msg(self, msg: RpcMsg) {
        let read_guard = self.servers.read().unwrap();
        let from_enable = match read_guard.get(msg.from) {
            None => false,
            Some(from_node) => from_node.enabled.load(Acquire)
        };

        // drop the message if the from node is disabled.
        if !from_enable { 
            println!("Drop the message from {}", msg.from);
            return; 
        }

        match msg.msg_type {
            MsgType::Broadcast => {
                println!("Broadcast message from {}", msg.from);
                read_guard.iter().enumerate()
                    .filter(|(id, _)| *id != msg.from)
                    .for_each(|(_, node)| {
                        node.tx.send(msg.clone()).unwrap();
                    });
            },
            MsgType::Unicast(to) => {
                println!("Broadcast message from {}", msg.from);
                match read_guard.get(to) {
                    // target node not found, drop the message.
                    None => {},
                    Some(node) => {
                        node.tx.send(msg).unwrap();
                    }
                }
            }
        }
    }
}

impl Network {
    pub fn new() -> Self {
        let (tx, rx) = tk_mpsc::unbounded_channel::<RpcMsg>();
        let servers = Arc::new(RwLock::new(Vec::new()));

        tokio::spawn(NetworkCore {
            servers: servers.clone()
        }.serve(rx));

        Self {
            servers,
            tx: Arc::new(tx),
        }
    }

    pub fn make_end(&self) -> Client {
        let (tx, rx) = tk_mpsc::unbounded_channel::<RpcMsg>();

        let server_id = {
            let mut write_guard = self.servers.write().unwrap();
            write_guard.push(Node {
                tx,
                enabled: AtomicBool::new(true)
            });
            write_guard.len() - 1
        };

        let services = ServiceTable::new();
        tokio::spawn(Server {
            ct_tx: self.tx.clone(),
            services: services.clone(),
        }.serve(rx));
        Client {
            id: server_id,
            ct_tx: self.tx.clone(),
            services
        }
    }

    // connect or disconnect a server from the network.
    pub fn connect(&self, id: ServerId, flag: bool) {
        self.servers.read().unwrap().get(id).unwrap()
            .enabled.store(flag, Release);
    }
}

impl Server {
    pub async fn serve(self, mut rx: Receiver<RpcMsg>) {
        while let Some(req) = rx.recv().await {
            tokio::spawn(self.clone().process_req(req));
        }
    }

    pub async fn process_req(self, req: RpcMsg) {
        let reply = match self.services.get_service(&req.method) {
            None => Reply::NotFound,
            Some(srv) => Reply::Success({
                let service = srv.clone();
                service.call(req.args).await
            })
        };

        let _ = req.reply_ch.send(bincode::serialize(&reply).unwrap());
    }
}

impl Client {
    pub fn add_service(&self, name: &str, service: Box<dyn Service>) {
        self.services.add_service(name, service);
    }

    pub fn unicast_call<A: RpcArgs, R: RpcReply>(
        &self, server_id: ServerId, method: &str, args: &A) -> 
        MapRx<Vec<u8>, Reply<R>> 
    {
        let (tx, rx) = tk_mpsc::unbounded_channel::<Vec<u8>>();
        self.ct_tx.send(RpcMsg {
            msg_type: MsgType::Unicast(server_id),
            from: self.id,
            method: String::from(method),
            args: bincode::serialize(args).unwrap(),
            reply_ch: Arc::new(tx)
        }).unwrap();

        MapRx::<Vec<u8>, Reply<R>>::new(rx)
    }

    pub fn broadcast_call<A: RpcArgs, R: RpcReply>(
        &self, method: &str, args: &A) -> 
        MapRx<Vec<u8>, Reply<R>>
    {
        let (tx, rx) = tk_mpsc::unbounded_channel::<Vec<u8>>();
        self.ct_tx.send(RpcMsg {
            msg_type: MsgType::Broadcast,
            from: self.id,
            method: String::from(method),
            args: bincode::serialize(args).unwrap(),
            reply_ch: Arc::new(tx)
        }).unwrap();

        MapRx::<Vec<u8>, Reply<R>>::new(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct HelloService;

    impl RpcArgs for String {}
    impl RpcReply for String {}
    
    #[async_trait]
    impl Service for HelloService {
        async fn call(&self, enc_args: Vec<u8>) -> Vec<u8> {
            let args = bincode::deserialize::<String>(&enc_args).unwrap();
            let reply = String::from(format!("Hello, {args}"));
            bincode::serialize(&reply).unwrap()
        }
    }

    #[tokio::test]
    async fn make_network() {
        let network = Network::new();
        let c1 = network.make_end();
        let c2 = network.make_end();

        c1.add_service("Hello", Box::new(HelloService {}));
        let args = String::from("Jack");
        match c2.unicast_call::<String, String>(c1.id, "Hello", &args).recv().await.unwrap() {
            Reply::NotFound => panic!("Service Hello should be found"),
            Reply::Success(msg) => {
                let expect = format!("Hello, {}", args);
                println!("{}", msg);
                println!("{}", expect);
                println!("msg len = {}, expect len = {}", msg.len(), expect.len());

                for c in msg.chars() {
                    print!("{} ", c);
                }

                assert!(msg.as_str() == expect);
            }
        };
    }
}
