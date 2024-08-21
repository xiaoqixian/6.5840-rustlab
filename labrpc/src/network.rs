// Date:   Fri Aug 16 15:36:01 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{collections::HashMap, sync::{Arc, RwLock}};
use tokio::sync::mpsc::{
    self as tk_mpsc,
};
use tokio::sync::oneshot::{
    self as tk_oneshot,
    Receiver as OneshotReceiver
};

use crate::{err::{Error, NetworkError}, msg::{Pack, ReplyTx, RpcReq}, CallResult};

use super::{UbTx, Rx};

type EndTable = Arc<RwLock<HashMap<u32, EndNode>>>;

/// EndNode is a channel for the network center to deliver
/// RPC request to the corresponding node.
/// It contains a Sender to send Pack to the node, 
/// and some other configuration fields.
struct EndNode {
    tx: UbTx<Pack>,
    connected: bool
}

pub struct Network {
    // tx: UbTx<Msg>,
    nodes: EndTable
}

/// NetworkHandle is for the node to perform some 
/// network related operations, i.e., send an unicast/broadcast
/// RPC request to the network.
pub struct NetworkHandle {
    id: u32,
    nodes: EndTable
}

impl Network {
    pub fn new() -> Self {
        // let (tx, rx) = tk_mpsc::unbounded_channel();
        // tokio::spawn(Self::run(rx));
        Self {
            // tx,
            nodes: Default::default()
        }
    }

    // async fn run(mut rx: UbRx<Msg>) {
    //     while let Some(msg) = rx.recv().await {
    //         
    //     }
    // }

    pub fn join(&self, node_tx: UbTx<Pack>) -> NetworkHandle {
        let new_node = EndNode {
            tx: node_tx,
            connected: true
        };
        let mut nodes = self.nodes.write().unwrap();
        let id = nodes.len() as u32;
        
        nodes.insert(id, new_node);
        NetworkHandle {
            id,
            nodes: self.nodes.clone()
        }
    }
}

impl NetworkHandle {
    pub fn unicast(&self, to: u32, req: RpcReq) 
        -> Result<OneshotReceiver<CallResult>, Error> {
        let nodes = self.nodes.read().unwrap();
        let me = nodes.get(&self.id).unwrap();

        if !me.connected {
            return Err(Error::NetworkError(NetworkError::Disconnected));
        }

        let peer = match nodes.get(&to) {
            None => return Err(Error::NetworkError(
                // TODO: add some delay
                NetworkError::PeerNotFound
            )),
            Some(peer) => peer
        };

        if !peer.connected {
            // TODO: add some delay
            return Err(Error::NetworkError(NetworkError::TimeOut));
        }

        let (tx, rx) = tk_oneshot::channel();
        let pack = Pack {
            req,
            reply_tx: ReplyTx::from(tx)
        };
        
        peer.tx.send(pack)?;
        Ok(rx)
    }

    /// If ok, return a async channel receiver to receive incoming results 
    /// from peer nodes.
    /// And the size of the channel is returned, so the caller can create 
    /// a wrapper channel with the same size.
    pub fn broadcast(&self, req: RpcReq) -> Result<(usize, Rx<CallResult>), Error> {
        let nodes = self.nodes.read().unwrap();
        let me = nodes.get(&self.id).unwrap();

        if !me.connected {
            return Err(Error::NetworkError(NetworkError::Disconnected));
        }

        let (tx, rx) = tk_mpsc::channel(nodes.len()-1);

        for (id, peer) in nodes.iter() {
            if id == &self.id || !peer.connected {
                continue;
            }
            let pack = Pack {
                req: req.clone(),
                reply_tx: ReplyTx::from(tx.clone())
            };
            peer.tx.send(pack)?;
        }
        Ok((nodes.len()-1, rx))
    }
}
