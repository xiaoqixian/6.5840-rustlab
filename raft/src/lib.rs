// Date:   Thu Aug 29 11:06:21 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod raft;
mod persist;
mod follower;
mod candidate;
mod leader;
mod common;
mod event;
mod service;
mod role;
mod logs;
mod utils;

type UbTx<T> = tokio::sync::mpsc::UnboundedSender<T>;
type UbRx<T> = tokio::sync::mpsc::UnboundedReceiver<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;

pub enum ApplyMsg {
    Command {
        index: usize,
        command: Vec<u8>
    },
    Snapshot {
        term: usize,
        index: usize,
        snapshot: Vec<u8>
    }
}

#[cfg(test)]
pub mod tests;
