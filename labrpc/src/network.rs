// Date:   Fri Aug 16 15:36:01 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}};
use tokio::sync::mpsc::{
    self as tk_mpsc,
};
use tokio::sync::oneshot::{
    self as tk_oneshot,
    Receiver as OneshotReceiver
};

use crate::{err::{Error, NetworkError}, msg::{Msg, ReplyTx, RpcReq}, CallResult};

use super::{UbTx, Rx};

type EndTable = Arc<RwLock<HashMap<u32, EndNode>>>;

/// EndNode is a channel for the network center to deliver
/// RPC request to the corresponding node.
/// It contains a Sender to send Pack to the node, 
/// and some other configuration fields.
struct EndNode {
    tx: UbTx<Msg>,
    connected: bool
}

#[derive(Clone)]
pub struct Network {
    // tx: UbTx<Msg>,
    nodes: EndTable,
    reliable: Arc<AtomicBool>
}

/// NetworkHandle is for the node to perform some 
/// network related operations, i.e., send an unicast/broadcast
/// RPC request to the network.
pub struct NetworkHandle {
    pub id: u32,
    net: Network
}

impl Network {
    pub fn new() -> Self {
        // let (tx, rx) = tk_mpsc::unbounded_channel();
        // tokio::spawn(Self::run(rx));
        Self {
            // tx,
            nodes: Default::default(),
            reliable: Arc::new(AtomicBool::new(true))
        }
    }

    #[inline]
    pub fn reliable(self, val: bool) -> Self {
        self.set_reliable(val);
        self
    }

    #[inline]
    pub fn set_reliable(&self, val: bool) {
        self.reliable.store(val, Ordering::Release);
    }

    pub fn join(&self, node_tx: UbTx<Msg>) -> NetworkHandle {
        let new_node = EndNode {
            tx: node_tx,
            connected: true
        };
        let mut nodes = self.nodes.write().unwrap();
        let id = nodes.len() as u32;
        
        nodes.insert(id, new_node);
        NetworkHandle {
            id,
            net: self.clone()
        }
    }
}

impl NetworkHandle {
    pub fn unicast(&self, to: u32, req: RpcReq) 
        -> Result<OneshotReceiver<CallResult>, Error> {
        let nodes = self.net.nodes.read().unwrap();
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
        let msg = Msg {
            req,
            reply_tx: ReplyTx::from(tx)
        };
        
        peer.tx.send(msg)?;
        Ok(rx)
    }

    /// If ok, return a async channel receiver to receive incoming results 
    /// from peer nodes.
    /// And the size of the channel is returned, so the caller can create 
    /// a wrapper channel with the same size.
    pub fn broadcast(&self, req: RpcReq) -> Result<(usize, Rx<CallResult>), Error> {
        let nodes = self.net.nodes.read().unwrap();
        let me = nodes.get(&self.id).unwrap();

        if !me.connected {
            return Err(Error::NetworkError(NetworkError::Disconnected));
        }

        let (tx, rx) = tk_mpsc::channel(nodes.len()-1);

        for (id, peer) in nodes.iter() {
            if id == &self.id || !peer.connected {
                continue;
            }
            let msg = Msg {
                req: req.clone(),
                reply_tx: ReplyTx::from(tx.clone())
            };
            peer.tx.send(msg)?;
        }
        Ok((nodes.len()-1, rx))
    }
}
