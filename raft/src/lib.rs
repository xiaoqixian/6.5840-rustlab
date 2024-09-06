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
mod service;

type UbTx<T> = tokio::sync::mpsc::UnboundedSender<T>;
type UbRx<T> = tokio::sync::mpsc::UnboundedReceiver<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;
type OneRx<T> = tokio::sync::oneshot::Receiver<T>;

use event::Event;
use follower::Follower;
use candidate::Candidate;
use leader::Leader;

enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader)
}

enum Outcome {
    BeCandidate,
    BeLeader,
    BeFollower
}

macro_rules! is_role {
    ($name: ident, $role: ident) => {
        pub fn $name(&self) -> bool {
            match self {
                Self::$role(_) => true,
                _ => false
            }
        }
    }
}

impl Role {
    async fn process(&mut self, ev: Event) -> Option<Outcome> {
        match self {
            Self::Follower(flw) => flw.process(ev).await,
            Self::Candidate(cd) => cd.process(ev).await,
            Self::Leader(ld) => ld.process(ev).await
        }
    }

    is_role!(is_follower, Follower);
    is_role!(is_candidate, Candidate);
    is_role!(is_leader, Leader);
}

#[cfg(test)]
pub mod tests;
