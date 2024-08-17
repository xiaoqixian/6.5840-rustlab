// Date:   Fri Aug 16 16:42:16 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::err::NetworkError;

use super::{
    Tx, Rx,
    service::ServiceContainer,
    network::Network,
    err::Error,
    msg::{MsgType, Msg}
};
use tokio::sync::mpsc as tk_mpsc;
use serde::{Serialize, de::DeserializeOwned};

pub struct End {
    id: u32,
    net_tx: Tx,
    services: ServiceContainer
}

struct EndServ {
    rx: Rx,
    services: ServiceContainer
}

impl EndServ {
    async fn run(mut self) {
        while let Some(_msg) = self.rx.recv().await {}
    }
}

impl End {
    pub fn new(network: &mut Network) -> Self {
        let (tx, rx) = tk_mpsc::unbounded_channel();
        let (id, net_tx) = network.join(tx);
        let services = ServiceContainer::new();
        let end_serv = EndServ {
            rx,
            services: services.clone()
        };

        tokio::spawn(end_serv.run());
        
        Self {
            id,
            net_tx,
            services
        }
    }

    async fn call<A, R>(&self, meth: &str, msg_type: MsgType, arg: A) -> Result<R, Error> 
        where A: Serialize, R: DeserializeOwned
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

        let (tx, rx) = tokio::sync::oneshot::channel();

        let msg = Msg {
            msg_type,
            from: self.id,
            cls,
            method,
            arg,
            reply_ch: tx
        };

        self.net_tx.send(msg)?;

        let reply = rx.await?;
        let res = bincode::deserialize_from(&reply[..])?;
        Ok(res)
    }
}
