// Date:   Thu Sep 05 16:01:48 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{fmt::Display, sync::{atomic::{AtomicUsize, Ordering}, RwLock}};

use crate::{
    role::Trans, service::{
        AppendEntriesArgs, AppendEntriesReply, 
        QueryEntryArgs, QueryEntryReply, 
        RequestVoteArgs, RequestVoteReply
    }, warn, OneTx, UbTx
};

pub const TO_CANDIDATE: Event = Event::Trans(Trans::ToCandidate);
pub const TO_LEADER: Event = Event::Trans(Trans::ToLeader);

pub enum Event {
    GetState(OneTx<(usize, bool)>),
    StartCmd {
        cmd: Vec<u8>,
        reply_tx: OneTx<Option<(usize, usize)>>
    },
    AppendEntries {
        args: AppendEntriesArgs,
        reply_tx: OneTx<AppendEntriesReply>,
    },
    RequestVote {
        args: RequestVoteArgs,
        reply_tx: OneTx<RequestVoteReply>
    },
    QueryEntry {
        args: QueryEntryArgs,
        reply_tx: OneTx<QueryEntryReply>
    },
    Trans(Trans),
    Kill,

    // follower related events
    HeartBeatTimeout,

    // candidate related events
    GrantVote {
        voter: usize,
    },
    
    OutdateCandidate {
        new_term: usize
    },

    ElectionTimeout,

    // leader related events
    UpdateCommit(usize),
}

pub struct EvQueue {
    ev_ch: RwLock<UbTx<Event>>,
    key: AtomicUsize,
    id: usize
}

impl EvQueue {
    pub fn new(ev_ch: UbTx<Event>, id: usize) -> Self {
        Self {
            ev_ch: RwLock::new(ev_ch),
            key: AtomicUsize::new(0),
            id
        }
    }

    /// put without pass key, only event from outside can 
    /// be put in this way, like Event::GetState.
    /// When the raft is killed, the ev channel will be closed, 
    /// then the Err(ev) will be returned.
    pub fn just_put(&self, ev: Event) -> Result<(), Event> {
        self.ev_ch.read().unwrap().send(ev).map_err(|e| e.0)
    }

    /// Put an event to the event queue, return Err(event) 
    /// if not success.
    pub fn put(&self, ev: Event, key: usize) -> Result<(), Event> {
        // reject events with unmatched key
        if self.key.load(Ordering::Acquire) != key {
            warn!("{self}: Discard event {ev} for unmatched key");
            return Err(ev);
        }

        match ev {
            Event::Trans(to) => {
                let ev_ch = self.ev_ch.write().unwrap();
                ev_ch.send(Event::Trans(to)).unwrap();
                self.key.fetch_add(1, Ordering::AcqRel);
            },
            ev => {
                let ev_ch = self.ev_ch.read().unwrap();
                ev_ch.send(ev).unwrap();
            }
        }
        Ok(())
    }

    pub fn key(&self) -> usize {
        self.key.load(Ordering::Acquire)
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Event::")?;
        match self {
            Self::GetState(_) => write!(f, "GetState"),
            Self::Kill => write!(f, "Kill"),
            Self::StartCmd {..} => write!(f, "StartCmd"),
            Self::AppendEntries {..} => write!(f, "AppendEntries"),
            Self::RequestVote {..} => write!(f, "RequestVote"),
            Self::QueryEntry {..} => write!(f, "QueryEntry"),
            Self::Trans(to) => write!(f, "Trans({to:?})"),
            Self::HeartBeatTimeout => write!(f, "HeartBeatTimeout"),
            Self::GrantVote {..} => write!(f, "GrantVote"),
            Self::OutdateCandidate {..} => write!(f, "OutdateCandidate"),
            Self::ElectionTimeout => write!(f, "ElectionTimeout"),
            Self::UpdateCommit(lci) => write!(f, "UpdateCommit({lci})")
        }
    }
}
impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Event::")?;
        match self {
            Self::GetState(_) => write!(f, "GetState"),
            Self::Kill => write!(f, "Kill"),
            Self::StartCmd {..} => write!(f, "StartCmd"),
            Self::AppendEntries {..} => write!(f, "AppendEntries"),
            Self::RequestVote {..} => write!(f, "RequestVote"),
            Self::QueryEntry {..} => write!(f, "QueryEntry"),
            Self::Trans(to) => write!(f, "Trans({to:?})"),
            Self::HeartBeatTimeout => write!(f, "HeartBeatTimeout"),
            Self::GrantVote {..} => write!(f, "GrantVote"),
            Self::OutdateCandidate {..} => write!(f, "OutdateCandidate"),
            Self::ElectionTimeout => write!(f, "ElectionTimeout"),
            Self::UpdateCommit(lci) => write!(f, "UpdateCommit({lci})")
        }
    }
}

impl Display for EvQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft {}", self.id)
    }
}
