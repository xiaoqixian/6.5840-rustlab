// Date:   Fri Aug 16 16:42:16 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{collections::HashMap, sync::Arc};

use crate::{
    err::{NetworkError, CLASS_NOT_FOUND, PEER_NOT_FOUND}, msg::{Msg, RpcReq}, service::Service, CallResult, Rx, UbTx
};

use super::{
    network::Network,
    err::Error,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::RwLock;

type ServiceContainer = Arc<RwLock<HashMap<String, Box<dyn Service>>>>;

#[derive(Clone, Default)]
pub struct Admin {
    services: ServiceContainer
}

#[derive(Clone)]
pub struct ClientEnd {
    id: u32,
    net_tx: UbTx<Msg>
}

#[derive(Default)]
pub struct Server {
    services: ServiceContainer
}

pub struct Client(HashMap<u32, ClientEnd>);

impl Client {
    pub fn new(list: HashMap<u32, ClientEnd>) -> Self {
        Self(list)
    }

    pub async fn unicast<A, R>(&self, to: u32, meth: &str, arg: A) -> Result<R, Error> 
        where A: Serialize, R: DeserializeOwned
    {
        match self.0.get(&to) {
            None => Err(PEER_NOT_FOUND),
            Some(peer) => peer.call(meth, arg).await
        }
    }

    pub async fn broadcast<A, R>(&self, meth: &str, arg: A) -> Result<Rx<Result<R, Error>>, Error> 
        where A: Serialize + Clone + Send + Sync + 'static,
            R: DeserializeOwned + Send + Sync + 'static
    {
        let (tx, rx) = tokio::sync::mpsc::channel(self.0.len());
        let meth = String::from(meth);
        for peer in self.0.values().cloned() {
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

impl Server {
    pub async fn dispatch(&self, req: RpcReq) -> CallResult {
        let services = self.services.read().await;
        match services.get(&req.cls) {
            None => Err(CLASS_NOT_FOUND),
            Some(h) => h.call(&req.method, &req.arg[..]).await
        }
    }
}

impl Admin {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub async fn join(&self, network: &Network) {
        network.join(Server {
            services: self.services.clone()
        }).await;
    }

    pub async fn add_service(&mut self, name: String, service: Box<dyn Service>) {
        self.services.write().await.insert(name, service);
    }
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
