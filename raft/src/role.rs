// Date:   Sat Sep 07 14:37:54 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use crate::debug;
use crate::event::{EvQueue, Event};
use crate::follower::Follower;
use crate::candidate::Candidate;
use crate::leader::Leader;
use crate::logs::Logs;
use crate::raft::RaftCore;

pub enum Role {
    // Nil provides a default value for Role, 
    // so the inner value can be taken out without 
    // violating ownership rules.
    Nil,
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader)
}

pub struct RoleCore {
    pub raft_core: RaftCore,
    pub logs: Logs,
    pub ev_q: RoleEvQueue
}

#[derive(Debug)]
pub enum Trans {
    ToCandidate,
    ToLeader,
    ToFollower
}

#[derive(Clone)]
pub struct RoleEvQueue {
    ev_q: Arc<EvQueue>,
    key: usize
}

impl RoleEvQueue {
    pub fn new(ev_q: Arc<EvQueue>, key: usize) -> Self {
        Self { ev_q, key }
    }

    pub fn put(&self, ev: Event) -> Result<(), Event> {
        self.ev_q.put(ev, self.key)
    }

    pub fn transfer(self) -> Self {
        let key = self.ev_q.key();
        Self {
            ev_q: self.ev_q,
            key
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Self::Nil
    }
}

macro_rules! forward {
    ($role: expr, $name: ident ( $($args: expr),* )) => {
        match $role {
            Role::Follower(flw) => flw.$name($($args),*),
            Role::Candidate(cd) => cd.$name($($args),*),
            Role::Leader(ld) => ld.$name($($args),*),
            Role::Nil => panic!("Role is Nil")
        }
    };
    ($role: expr, $name: ident ( $($args: expr),* ).await) => {
        match $role {
            Role::Follower(flw) => flw.$name($($args),*).await,
            Role::Candidate(cd) => cd.$name($($args),*).await,
            Role::Leader(ld) => ld.$name($($args),*).await,
            Role::Nil => panic!("Role is Nil")
        }
    }
}

impl Role {
    pub async fn process(&mut self, ev: Event) {
        let trans = forward!(self, process(ev).await);

        // The role may need to go through some role transformation.
        if let Some(trans) = trans {
            let role = std::mem::take(self);

            // only the follower can change the term by itself, 
            // candidate and leader cannot change the term, the 
            // only time they need to change the term is when 
            // they need to become a follower.
            *self = match (role, trans) {
                (Self::Follower(flw), Trans::ToCandidate) => {
                    let core = flw.stop();
                    // debug!("[Raft {}]: be candidate", core.raft_core.me);
                    Self::Candidate(Candidate::from(core))
                },
                (Self::Candidate(cd), Trans::ToLeader) => {
                    let core = cd.stop();
                    // debug!("[Raft {}]: be leader", core.raft_core.me);
                    Self::Leader(Leader::from(core))
                },
                (Self::Candidate(cd), Trans::ToFollower) => {
                    let core = cd.stop();
                    // debug!("[Raft {}]: candidate to follower", core.raft_core.me);
                    Self::Follower(Follower::from(core))
                },
                (Self::Leader(ld), Trans::ToFollower) => {
                    let core = ld.stop();
                    // debug!("[Raft {}]: leader to follower", core.raft_core.me);
                    Self::Follower(Follower::from(core))
                },
                (r, t) => panic!("Unexpected combination of 
                    transformation {t:?} and role {r}.")
            }
        }
    }

    pub fn stop(self) {
        forward!(self, stop());
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Follower(_) => write!(f, "Follower"),
            Self::Candidate(_) => write!(f, "Candidate"),
            Self::Leader(_) => write!(f, "Leader"),
            Self::Nil => write!(f, "Nil"),
        }
    }
}
