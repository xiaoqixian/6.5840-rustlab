// Date:   Thu Aug 29 11:06:21 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod raft;
pub mod msg;
pub(crate) mod persist;

mod follower;
mod candidate;
mod leader;
mod utils;
mod event;

type UbTx<T> = tokio::sync::mpsc::UnboundedSender<T>;
type UbRx<T> = tokio::sync::mpsc::UnboundedReceiver<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;
type OneRx<T> = tokio::sync::oneshot::Receiver<T>;

use follower::Follower;
use candidate::Candidate;
use leader::Leader;

enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader)
}

#[cfg(test)]
pub mod tests;
