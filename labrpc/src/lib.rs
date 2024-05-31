// Date:   Tue May 14 22:19:34 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::collections::HashMap;
use std::sync::atomic::{ AtomicBool, AtomicU64, Ordering::* };
use std::sync::{Arc, RwLock};

use tokio::sync::{mpsc as tk_mpsc, oneshot as tk_oneshot};
use async_trait::async_trait;
use serde::{Serialize, Deserialize, de::DeserializeOwned};

mod map;
mod channel;

use channel::ReplyReceiver;

type Sender<T> = tk_mpsc::UnboundedSender<T>;
type Receiver<T> = tk_mpsc::UnboundedReceiver<T>;

pub type ServerId = usize;

pub trait RpcArgs: Serialize + DeserializeOwned {}
pub trait RpcReply: Serialize + DeserializeOwned {}

// R represents the reply value type
#[derive(Serialize, Deserialize)]
pub enum Reply<R> {
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

pub enum ReplyChOp {
    SendMsg(u64, Vec<u8>),
    AddCh(u64, Sender<Vec<u8>>, tk_oneshot::Sender<()>),
    RemoveCh(u64)
}

#[derive(Clone)]
enum MsgType {
    Unicast(ServerId),
    Broadcast
}

#[derive(Clone)]
struct RpcReq {
    req_id: u64,
    name: String,
    args: Vec<u8>
}

#[derive(Clone)]
struct RpcResp {
    req_id: u64,
    reply: Vec<u8>
}

#[derive(Clone)]
enum RpcType {
    Request(RpcReq),
    Response(RpcResp)
}

#[derive(Clone)]
struct RpcMsg {
    from: ServerId,
    msg_type: MsgType,
    rpc: RpcType
}

#[async_trait]
pub trait Service: Send + Sync {
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
    id: ServerId,
    ct_tx: Arc<Sender<RpcMsg>>,
    services: ServiceTable,
}

pub struct Client {
    id: ServerId,
    ct_tx: Arc<Sender<RpcMsg>>,
    services: ServiceTable,
    req_cnt: AtomicU64,
    reply_op_tx: Arc<Sender<ReplyChOp>>
}

pub struct Network {
    servers: Arc<RwLock<Vec<Arc<Node>>>>,
    tx: Arc<Sender<RpcMsg>>,
}
#[derive(Clone)]
struct NetworkCore {
    servers: Arc<RwLock<Vec<Arc<Node>>>>,
    reliable: bool,
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
        let from_enable = match self.servers.read().unwrap().get(msg.from) {
            None => false,
            Some(from_node) => from_node.enabled.load(Acquire)
        };

        // drop the message if the from node is disabled.
        if !from_enable { 
            return; 
        }


        match msg.msg_type {
            MsgType::Broadcast => {
                println!("Broadcast message from {}", msg.from);
                self.servers.read().unwrap().iter().enumerate()
                    .filter(|(id, node)|
                        *id != msg.from && node.enabled.load(Acquire)
                    )
                    .for_each(|(_, node)| {
                        tokio::spawn(Self::node_send_msg(
                                node.clone(), msg.clone(), self.reliable
                        ));
                    });
            },
            MsgType::Unicast(to) => {
                println!("Broadcast message from {}", msg.from);
                match self.servers.read().unwrap().get(to) {
                    // target node not found, drop the message.
                    None => {},
                    Some(node) => {
                        tokio::spawn(Self::node_send_msg(
                                node.clone(), msg, self.reliable
                        ));
                    }
                }
            }
        }
    }

    async fn node_send_msg(node: Arc<Node>, msg: RpcMsg, reliable: bool) {
        node.send_msg(msg, reliable).await;
    }
}

impl Network {
    pub fn new(reliable: bool) -> Self {
        let (tx, rx) = tk_mpsc::unbounded_channel::<RpcMsg>();
        let servers = Arc::new(RwLock::new(Vec::new()));

        tokio::spawn(NetworkCore {
            servers: servers.clone(),
            reliable
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
            write_guard.push(Arc::new(Node {
                tx,
                enabled: AtomicBool::new(true)
            }));
            write_guard.len() - 1
        };

        let services = ServiceTable::new();

        let (reply_op_tx, reply_op_rx) = tk_mpsc::unbounded_channel::<ReplyChOp>();
        let (srv_tx, srv_rx) = tk_mpsc::unbounded_channel::<RpcMsg>();
        let reply_op_tx = Arc::new(reply_op_tx);

        tokio::spawn(process_reply(reply_op_rx));
        tokio::spawn(msg_route(rx, srv_tx, reply_op_tx.clone()));

        tokio::spawn(Server {
            id: server_id,
            ct_tx: self.tx.clone(),
            services: services.clone(),
        }.serve(srv_rx));

        Client {
            id: server_id,
            ct_tx: self.tx.clone(),
            services,
            req_cnt: AtomicU64::new(0),
            reply_op_tx,
        }
    }

    // connect or disconnect a server from the network.
    pub fn connect(&self, id: ServerId, flag: bool) {
        self.servers.read().unwrap().get(id).unwrap()
            .enabled.store(flag, Release);
    }

}

impl Node {
    async fn send_msg(&self, msg: RpcMsg, reliable: bool) {
        if !self.enabled.load(Acquire) { return; }
        
        if !reliable {
            // short delay
            tokio::time::sleep(std::time::Duration::from_millis(
                    rand::random::<u64>() % 27)).await;

            // 10% of possibility the message is dropped.
            if rand::random::<u32>() % 1000 < 100 {
                return;
            }
        }

        self.tx.send(msg).unwrap();
    }
}

impl Server {
    pub async fn serve(self, mut rx: Receiver<RpcMsg>) {
        while let Some(req) = rx.recv().await {
            tokio::spawn(self.clone().process_req(req));
        }
    }

    pub async fn process_req(self, msg: RpcMsg) {
        let req = match msg.rpc {
            RpcType::Response(_) => 
                panic!("RPC request expected in channel to the server"),
            RpcType::Request(req) => req
        };

        let reply = match self.services.get_service(&req.name) {
            None => Reply::NotFound,
            Some(srv) => Reply::Success({
                let service = srv.clone();
                service.call(req.args).await
            })
        };

        self.ct_tx.send(RpcMsg {
            from: self.id,
            msg_type: MsgType::Unicast(msg.from),
            rpc: RpcType::Response(RpcResp {
                req_id: req.req_id,
                reply: bincode::serialize(&reply).unwrap()
            })
        }).unwrap();
    }
}

impl Client {
    pub fn add_service(&self, name: &str, service: Box<dyn Service>) {
        self.services.add_service(name, service);
    }

    async fn call<A: RpcArgs, R: RpcReply>(
        &self, msg_type: MsgType, name: &str, args: &A) -> 
        ReplyReceiver<R>
    {
        let (tx, rx) = tk_mpsc::unbounded_channel::<Vec<u8>>();
        let req_id = self.req_cnt.fetch_add(1, AcqRel);

        let (sig_tx, sig_rx) = tk_oneshot::channel::<()>();
        self.reply_op_tx.send(ReplyChOp::AddCh(req_id, tx, sig_tx)).unwrap();
        sig_rx.await.unwrap();

        self.ct_tx.send(RpcMsg {
            msg_type,
            from: self.id,
            rpc: RpcType::Request(RpcReq {
                req_id,
                name: String::from(name),
                args: bincode::serialize(args).unwrap()
            })
        }).unwrap();

        ReplyReceiver::new(req_id, rx, self.reply_op_tx.clone())
    }

    pub async fn unicast_call<A: RpcArgs, R: RpcReply>(
        &self, server_id: ServerId, name: &str, args: &A) -> 
        ReplyReceiver<R>
    {
        self.call(MsgType::Unicast(server_id), name, args).await
    }

    pub async fn broadcast_call<A: RpcArgs, R: RpcReply>(
        &self, name: &str, args: &A) -> 
        ReplyReceiver<R>
    {
        self.call(MsgType::Broadcast, name, args).await
    }
}

async fn msg_route(mut rx: Receiver<RpcMsg>, 
    srv_tx: Sender<RpcMsg>, reply_op_tx: Arc<Sender<ReplyChOp>>) 
{
    while let Some(msg) = rx.recv().await {
        match msg.rpc {
            RpcType::Response(resp) => {
                reply_op_tx.send(ReplyChOp::SendMsg(resp.req_id, resp.reply)).unwrap();
            },
            RpcType::Request(_) => {
                let _ = srv_tx.send(msg);
            }
        }
    }
}

async fn process_reply(mut rx: Receiver<ReplyChOp>) {
    let mut ch_map = HashMap::<u64, Sender<Vec<u8>>>::new();
    while let Some(op) = rx.recv().await {
        match op {
            ReplyChOp::AddCh(id, tx, sig_tx) => {
                assert!(ch_map.insert(id, tx).is_none());
                // signal the reply channel is registered.
                sig_tx.send(()).unwrap();
            }
            ReplyChOp::RemoveCh(id) => assert!(ch_map.remove(&id).is_some()),
            ReplyChOp::SendMsg(id, reply) => {
                if let Some(tx) = ch_map.get(&id) {
                    let _ = tx.send(reply);
                }
            }
        }
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

    struct Env {
        net: Network,
        ends: Vec<Client>
    }

    fn make_network(n: usize, reliable: bool) -> Env {
        let net = Network::new(reliable);
        let ends = (0..n)
            .into_iter()
            .map(|_| net.make_end())
            .collect::<Vec<Client>>();
        Env {
            net,
            ends
        }
    }

    #[tokio::test]
    async fn unicast_test() {
        let env = make_network(2, true);
        let c1 = &env.ends[0];
        let c2 = &env.ends[1];

        c1.add_service("Hello", Box::new(HelloService {}));
        let args = String::from("Jack");
        match c2.unicast_call::<String, String>(c1.id, "Hello", &args)
            .await.recv().await.unwrap() {
            Reply::NotFound => panic!("Service Hello should be found"),
            Reply::Success(msg) => 
                assert!(msg.as_str() == format!("Hello, {}", args))
        };
    }

    #[tokio::test]
    async fn broadcast_test() {
        let env = make_network(5, true);
        env.ends.iter()
            .for_each(|clt| 
                clt.add_service("Hello", Box::new(HelloService {})));

        let args = String::from("Jack");
        let c1 = &env.ends[0];
        
        let mut replier = c1.broadcast_call::<String, String>(
            "Hello", &args).await;
        let mut cnt: usize = 0;

        while let Some(reply) = replier.recv().await {
            match reply {
                Reply::NotFound => panic!("Service Hello should be found"),
                Reply::Success(msg) => 
                    assert!(msg.as_str() == format!("Hello, {}", args))
            }
            cnt += 1;
        }
        assert_eq!(cnt, 4);

        // disconnect server1.
        env.net.connect(1, false);

        cnt = 0;
        while let Some(reply) = replier.recv().await {
            match reply {
                Reply::NotFound => panic!("Service Hello should be found"),
                Reply::Success(msg) => 
                    assert!(msg.as_str() == format!("Hello, {}", args))
            }
            cnt += 1;
        }
        assert_eq!(cnt, 3);
    }
}

