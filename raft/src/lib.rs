// Date:   Thu Aug 29 11:06:21 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod raft;
mod persist;

type UbTx<T> = tokio::sync::mpsc::UnboundedSender<T>;
type UbRx<T> = tokio::sync::mpsc::UnboundedReceiver<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;
// type OneRx<T> = tokio::sync::oneshot::Receiver<T>;

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
