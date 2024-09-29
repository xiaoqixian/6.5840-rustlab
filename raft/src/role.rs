// Date:   Sat Sep 07 14:37:54 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use crate::event::{EvQueue, Event};
use crate::follower::Follower;
use crate::candidate::Candidate;
use crate::leader::Leader;

pub enum Role {
    // Nil provides a default value for Role, 
    // so the inner value can be taken out without 
    // violating ownership rules.
    Nil,
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader)
}

#[derive(Debug)]
pub enum Trans {
    ToCandidate,
    ToLeader,
    ToFollower {
        new_term: Option<usize>
    }
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

    pub async fn put(&self, ev: Event) -> Result<(), Event> {
        self.ev_q.put(ev, self.key).await
    }

    pub fn transfer(self) -> Self {
        let key = self.ev_q.key();
        Self {
            ev_q: self.ev_q,
            key
        }
    }
}

// macro_rules! is_role {
//     ($name: ident, $role: ident) => {
//         pub fn $name(&self) -> bool {
//             match self {
//                 Self::$role(_) => true,
//                 _ => false
//             }
//         }
//     }
// }

impl Default for Role {
    fn default() -> Self {
        Self::Nil
    }
}

impl Role {
    pub async fn process(&mut self, ev: Event) {
        let trans = match self {
            Self::Follower(flw) => flw.process(ev).await,
            Self::Candidate(cd) => cd.process(ev).await,
            Self::Leader(ld) => ld.process(ev).await,
            Self::Nil => panic!("Role is Nil")
        };

        // The role may need to go through some role transformation.
        if let Some(trans) = trans {
            let mut role = std::mem::take(self);
            role.stop().await;

            // only the follower can change the term by itself, 
            // candidate and leader cannot change the term, the 
            // only time they need to change the term is when 
            // they need to become a follower.
            *self = match (role, trans) {
                (Self::Follower(mut flw), Trans::ToCandidate) => {
                    flw.core.term += 1;
                    Self::Candidate(Candidate::from_follower(flw).await)
                },
                (Self::Candidate(cd), Trans::ToLeader) => {
                    Self::Leader(Leader::from_candidate(cd).await)
                },
                (Self::Candidate(mut cd), Trans::ToFollower {new_term}) => {
                    if let Some(new_term) = new_term {
                        cd.core.term = new_term;
                    }
                    Self::Follower(Follower::from_candidate(cd).await)
                },
                (Self::Leader(mut ld), Trans::ToFollower {new_term}) => {
                    if let Some(new_term) = new_term {
                        ld.core.term = new_term;
                    }
                    Self::Follower(Follower::from_leader(ld).await)
                },
                (r, t) => panic!("Unexpected combination of 
                    transformation {t:?} and role {r}.")
            }
        }
    }

    pub async fn stop(&mut self) {
        match self {
            Self::Follower(flw) => flw.stop().await,
            Self::Candidate(cd) => cd.stop().await,
            Self::Leader(ld) => ld.stop().await,
            Self::Nil => panic!("Role is Nil")
        }
    }

    // is_role!(is_follower, Follower);
    // is_role!(is_candidate, Candidate);
    // is_role!(is_leader, Leader);
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
