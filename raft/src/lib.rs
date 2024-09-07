// Date:   Thu Aug 29 11:06:21 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod raft;
mod msg;
mod persist;
mod follower;
mod candidate;
mod leader;
mod utils;
mod event;
mod service;
mod role;
mod log;

type UbTx<T> = tokio::sync::mpsc::UnboundedSender<T>;
type UbRx<T> = tokio::sync::mpsc::UnboundedReceiver<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;
type OneRx<T> = tokio::sync::oneshot::Receiver<T>;

#[cfg(test)]
pub mod tests;
