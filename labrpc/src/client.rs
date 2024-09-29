// Date:   Wed Aug 28 14:31:26 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{
    err::{Error, DISCONNECTED, TIMEOUT}, server::Server, Idx, Key, Msg, RpcReq, Service, UbTx
};

use serde::{Serialize, de::DeserializeOwned};

/// A ClientEnd represents an end-to-end channel 
/// from a client to a server.
///
/// @from:   the client ID
/// @to:     the server ID
/// @net_tx: the message channel to the network center
#[derive(Clone)]
pub struct ClientEnd {
    key: Key,
    from: Idx,
    to: Idx,
    net_tx: UbTx<Msg>
}

pub struct Client {
    key: Key,
    idx: Idx,
    n: usize,
    server: Server,
    net_tx: UbTx<Msg>
}

pub struct PeersIter {
    key: Key,
    idx: Idx,
    n: usize,
    net_tx: UbTx<Msg>,
    pos: usize,
}

impl Client {
    pub(crate) fn new(key: Key, idx: Idx, n: usize, 
        server: Server, 
        net_tx: UbTx<Msg>) -> Self {
        Self { key, idx, n, server, net_tx }
    }

    pub fn n(&self) -> usize {
        self.n
    }

    pub async fn add_service(&self, name: String, service: Box<dyn Service>) {
        self.server.add_service(name, service).await;
    }

    pub fn peers(&self) -> PeersIter {
        PeersIter {
            key: self.key,
            idx: self.idx,
            n: self.n,
            net_tx: self.net_tx.clone(),
            pos: 0
        }
    }
}

impl Iterator for PeersIter {
    type Item = ClientEnd;

    fn next(&mut self) -> Option<Self::Item> {
        // skip self node.
        if self.pos == self.idx {
            self.pos += 1;
        }

        if self.pos < self.n {
            let ret = ClientEnd {
                key: self.key,
                from: self.idx,
                to: self.pos,
                net_tx: self.net_tx.clone()
            };
            self.pos += 1;
            Some(ret)
        } else {
            None
        }
    }
}

impl ClientEnd {
    fn gen_req<A>(meth: &str, arg: &A) -> Result<RpcReq, Error> 
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
        let arg = match bincode::serialize(arg) {
            Ok(arg) => arg,
            Err(e) => panic!("Unexpected bincode serialization error {e:?}")
        };

        Ok(RpcReq { cls, method, arg })
    }

    pub fn to(&self) -> Idx {
        self.to
    }

    pub async fn call<A, R>(&self, meth: &str, arg: &A) -> Result<R, Error> 
        where A: Serialize, R: DeserializeOwned
    {
        let req = Self::gen_req(meth, arg)?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = Msg {
            key: self.key,
            from: self.from,
            to: self.to,
            req,
            reply_tx: tx
        };
        // the network is closed.
        if let Err(_) = self.net_tx.send(msg) {
            return Err(DISCONNECTED);
        }

        let res_enc = match rx.await {
            Ok(r) => r?,
            Err(_) => {
                // the sender is dropped without sending, 
                // which means the message is dropped.
                return Err(TIMEOUT);
            }
        };
        
        let res: R = bincode::deserialize(&res_enc[..]).unwrap();
        Ok(res)
    }
}

