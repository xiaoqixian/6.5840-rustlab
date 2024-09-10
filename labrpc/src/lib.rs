// Date:   Thu Aug 15 13:18:58 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod network;
pub mod err;
pub mod client;
mod server;

use tokio::sync::mpsc as tk_mpsc;

type UbRx<T> = tk_mpsc::UnboundedReceiver<T>;
type UbTx<T> = tk_mpsc::UnboundedSender<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;

// Idx represents the index of a raft node.
type Idx = usize;
// Key represents an unique ID of a client or server,
// I use type alias to differ the struct fields.
type Key = usize;

pub type CallResult = Result<Vec<u8>, err::Error>;

#[async_trait::async_trait]
pub trait Service: Send + Sync {
    async fn call(&self, method: &str, arg: &[u8]) -> CallResult;
}

#[derive(Clone)]
pub(crate) struct RpcReq {
    pub cls: String,
    pub method: String,
    pub arg: Vec<u8>,
}

pub(crate) struct Msg {
    pub key: Key,
    pub from: Idx,
    pub to: Idx,
    pub req: RpcReq,
    pub reply_tx: OneTx<CallResult>
}

// #[cfg(test)]
// mod tests {
//     use std::time::Duration;
//
//     use labrpc_macros::rpc;
//     use crate::{err::{self, DISCONNECTED, TIMEOUT}, CallResult, Service};
//
//     struct Hello;
//
//     #[rpc(Service, CallResult, err)]
//     impl Hello {
//         pub fn hello(&self, name: String) -> String {
//             format!("Hello, {name}")
//         }
//
//         pub async fn no_response(&self, _: ()) {
//             // sleep a long time to simulate the situation where
//             // the service is unresponsive.
//             tokio::time::sleep(Duration::from_secs(120)).await;
//         }
//     }
// }
