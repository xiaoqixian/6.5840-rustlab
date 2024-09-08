// Date:   Wed Aug 28 14:31:26 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, Ordering}, Arc};

use crate::{
    err::{Error, PEER_NOT_FOUND, DISCONNECTED}, 
    Msg, RpcReq, 
    network::{Peers, ServiceContainer}, 
    Rx, Service, UbTx
};

use serde::{Serialize, de::DeserializeOwned};


/// A ClientEnd is associated with a server id, 
/// it can start a RPC request to the corresponding 
/// RPC server.
#[derive(Clone)]
pub(crate) struct ClientEnd {
    id: usize,
    net_tx: UbTx<Msg>
}

/// A Client is a collection of ClientEnds, 
/// so you can use it to unicast a request to a server, 
/// or broadcast your requests to all servers.
pub struct Client {
    id: usize,
    peers: Peers,
    services: ServiceContainer,
    // if not connected, the client is unable to 
    // send any request to the network.
    connected: Arc<AtomicBool>
}

impl ClientEnd {
    pub fn new(id: usize, net_tx: UbTx<Msg>) -> Self {
        Self { id, net_tx }
    }

    fn gen_req<A>(meth: &str, arg: A) -> Result<RpcReq, Error> 
        where A: Serialize
    {
        let (cls, method) = {
            let mut splits = meth.split('.').into_iter();
            (
                String::from(splits.next().expect(
                    &format!("Invalid meth: {meth}")
                )),
                String::from(splits.next().expect(
                    &format!("Invalid meth: {meth}")
                )),
            )
        };
        let arg = match bincode::serialize(&arg) {
            Ok(arg) => arg,
            Err(e) => panic!("Unexpected bincode serialization error {e:?}")
        };

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
        
        let res_enc = rx.await.unwrap()?;
        let res = bincode::deserialize_from(&res_enc[..]).unwrap();
        Ok(res)
    }
}

impl Client {
    pub(crate) fn new(id: usize, peers: Peers, services: ServiceContainer, 
        connected: Arc<AtomicBool>) -> Self {
        Self { id, peers, services, connected }
    }

    #[inline]
    fn connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    // #[inline]
    // pub fn id(&self) -> usize {
    //     self.id
    // }
    //
    #[inline]
    pub async fn add_service(&mut self, name: String, service: Box<dyn Service>) {
        self.services.write().await.insert(name, service);
    }

    #[inline]
    pub async fn size(&self) -> usize {
        self.peers.read().await.len()
    }

    pub async fn unicast<A, R>(&self, to: usize, meth: &str, arg: A) -> Result<R, Error> 
        where A: Serialize, R: DeserializeOwned
    {
        if !self.connected() {
            return Err(DISCONNECTED);
        }

        match self.peers.read().await.get(&to) {
            None => Err(PEER_NOT_FOUND),
            Some(peer) => peer.call(meth, arg).await
        }
    }

    pub async fn multicast<A, R>(&self, to: &[usize], meth: &str, arg: A) -> Result<Rx<Result<R, Error>>, Error> 
        where A: Serialize + Clone + Send + Sync + 'static,
            R: DeserializeOwned + Send + Sync + 'static
    {
        if !self.connected() {
            return Err(DISCONNECTED);
        }

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

    /// Broadcast a message to the cluster, return a channel to receive 
    /// responses and the size of the cluster (so the caller can calculate
    /// the quorum).
    pub async fn broadcast<A, R>(&self, meth: &str, arg: A) -> Result<(Rx<Result<R, Error>>, usize), Error> 
        where A: Serialize + Clone + Send + Sync + 'static,
            R: DeserializeOwned + Send + Sync + 'static
    {
        if !self.connected() {
            return Err(DISCONNECTED);
        }

        let peers = self.peers.read().await;
        let n = peers.len();
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
        Ok((rx, n))
    }
}
