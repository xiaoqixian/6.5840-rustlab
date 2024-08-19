// Date:   Fri Aug 16 16:42:16 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{collections::HashMap, sync::Arc};

use crate::{
    err::{NetworkError, ServiceError}, 
    msg::{Pack, RpcReq}, 
    network::NetworkHandle, 
    service::Service
};

use super::{
    UbRx, Rx,
    network::Network,
    err::Error,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::RwLock;

type ServiceContainer = Arc<RwLock<HashMap<String, Box<dyn Service>>>>;

pub struct End {
    net: NetworkHandle,
    services: ServiceContainer
}

struct EndServ {
    rx: UbRx<Pack>,
    services: ServiceContainer
}

impl EndServ {
    async fn run(mut self) {
        while let Some(pack) = self.rx.recv().await {
            let Pack { req, reply_tx } = pack;
            let services = self.services.read().await;

            let res = match services.get(&req.cls) {
                Some(host) => {
                    host.call(&req.method, &req.arg[..])
                }
                None => Err(ServiceError::ClassNotFound)
            };

            reply_tx.send(res).await;
        }
    }
}

impl End {
    pub fn new(network: &Network) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let services = ServiceContainer::default();

        tokio::spawn(EndServ {
            rx,
            services: services.clone()
        }.run());

        Self {
            net: network.join(tx),
            services
        }
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

    pub async fn unicast<A, R>(&self, to: u32, meth: &str, arg: A) -> Result<R, Error> 
        where A: Serialize, R: DeserializeOwned
    {
        let req = Self::gen_req(meth, arg)?;
        let res = self.net.unicast(to, req)?.await?;
        let res = bincode::deserialize_from(&res[..])?;
        Ok(res)
    }

    pub async fn broadcast<A, R>(&self, meth: &str, arg: A) -> Result<Rx<R>, Error> 
        where A: Serialize, R: DeserializeOwned + Send + 'static
    {
        let req = Self::gen_req(meth, arg)?;
        let (len, mut res_rx) = self.net.broadcast(req)?;
        let (tx, rx) = tokio::sync::mpsc::channel(len);
        tokio::spawn(async move {
            while let Some(res) = res_rx.recv().await {
                let res = bincode::deserialize_from(&res[..])
                    .expect("broadcast: result deserialization error");
                tx.send(res).await.unwrap();
            }
        });
        Ok(rx)
    }
}
