// Date:   Wed Aug 28 14:31:26 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{
    err::{Error, NetworkError, PEER_NOT_FOUND}, 
    msg::{Msg, RpcReq}, Rx, UbTx,
    network::{Peers, ServiceContainer},
    Service
};

use serde::{Serialize, de::DeserializeOwned};


/// A ClientEnd is associated with a server id, 
/// it can start a RPC request to the corresponding 
/// RPC server.
#[derive(Clone)]
pub struct ClientEnd {
    id: u32,
    net_tx: UbTx<Msg>
}

/// A Client is a collection of ClientEnds, 
/// so you can use it to unicast a request to a server, 
/// or broadcast your requests to all servers.
pub struct Client {
    id: u32,
    peers: Peers,
    services: ServiceContainer
}

impl ClientEnd {
    pub fn new(id: u32, net_tx: UbTx<Msg>) -> Self {
        Self { id, net_tx }
    }

    fn gen_req<A>(meth: &str, arg: A) -> Result<RpcReq, Error> 
        where A: Serialize
    {
        let (cls, method) = {
            let mut splits = meth.split('.').into_iter();
            let mut parse_str = || {
                match splits.next() {
                    None => Err(Error::NetworkError(
                        NetworkError::MethError(
                            format!("Invalid meth: {meth}")
                        )
                    )),
                    Some(s) => Ok(String::from(s))
                }
            };

            (parse_str()?, parse_str()?)
        };
        let arg = bincode::serialize(&arg)?;

        Ok(RpcReq { cls, method, arg })
    }

    pub async fn call<A, R>(&self, meth: &str, arg: A) -> Result<R, Error> 
        where A: Serialize, R: DeserializeOwned
    {
        let req = Self::gen_req(meth, arg)?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg {
            end_id: self.id,
            req,
            reply_tx: tx
        };
        self.net_tx.send(msg).unwrap();
        
        let res_enc = rx.await??;
        let res = bincode::deserialize_from(&res_enc[..]).unwrap();
        Ok(res)
    }
}

impl Client {
    pub fn new(id: u32, peers: Peers, services: ServiceContainer) -> Self {
        Self { id, peers, services }
    }

    #[inline]
    pub fn id(&self) -> u32 {
        self.id
    }

    #[inline]
    pub async fn add_service(&mut self, name: String, service: Box<dyn Service>) {
        self.services.write().await.insert(name, service);
    }

    pub async fn unicast<A, R>(&self, to: u32, meth: &str, arg: A) -> Result<R, Error> 
        where A: Serialize, R: DeserializeOwned
    {
        match self.peers.read().await.get(&to) {
            None => Err(PEER_NOT_FOUND),
            Some(peer) => peer.call(meth, arg).await
        }
    }

    pub async fn multicast<A, R>(&self, to: &[u32], meth: &str, arg: A) -> Result<Rx<Result<R, Error>>, Error> 
        where A: Serialize + Clone + Send + Sync + 'static,
            R: DeserializeOwned + Send + Sync + 'static
    {
        let (tx, rx) = tokio::sync::mpsc::channel(to.len());
        let meth = String::from(meth);
        let peers = self.peers.read().await;
        
        for id in to {
            match peers.get(id) {
                None => tx.send(Err(PEER_NOT_FOUND)).await.unwrap(),
                Some(peer) => {
                    let peer = peer.clone();
                    let tx = tx.clone();
                    let arg = arg.clone();
                    let meth = meth.clone();
                    tokio::spawn(async move {
                        let res = peer.call(&meth, arg).await;
                        tx.send(res).await.unwrap();
                    });
                }
            }
        }
        Ok(rx)
    }

    pub async fn broadcast<A, R>(&self, meth: &str, arg: A) -> Result<Rx<Result<R, Error>>, Error> 
        where A: Serialize + Clone + Send + Sync + 'static,
            R: DeserializeOwned + Send + Sync + 'static
    {
        let peers = self.peers.read().await;
        let (tx, rx) = tokio::sync::mpsc::channel(peers.len());
        let meth = String::from(meth);

        for (id, peer) in peers.iter() {
            // avoid sending msg to myself
            if *id == self.id { continue; }

            let peer = peer.clone();
            let tx = tx.clone();
            let arg = arg.clone();
            let meth = meth.clone();
            tokio::spawn(async move {
                let res = peer.call(&meth, arg).await;
                tx.send(res).await.unwrap();
            });
        }
        Ok(rx)
    }
}
