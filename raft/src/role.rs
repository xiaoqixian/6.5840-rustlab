// Date:   Sat Sep 07 14:37:54 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::event::Event;
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
    ToFollower
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
            let role = std::mem::take(self);
            role.stop().await;

            *self = match (role, trans) {
                (Self::Follower(flw), Trans::ToCandidate) => {
                    Self::Candidate(Candidate::from(flw))
                },
                (Self::Candidate(cd), Trans::ToLeader) => {
                    Self::Leader(Leader::from(cd))
                },
                (Self::Candidate(cd), Trans::ToFollower) => {
                    Self::Follower(Follower::from(cd))
                },
                (Self::Leader(ld), Trans::ToFollower) => {
                    Self::Follower(Follower::from(ld))
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

    is_role!(is_follower, Follower);
    is_role!(is_candidate, Candidate);
    is_role!(is_leader, Leader);
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