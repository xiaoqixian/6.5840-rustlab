// Date:   Thu Sep 05 16:01:48 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::RwLock;

use crate::{
    role::Trans, service::{
        AppendEntriesArgs, 
        AppendEntriesReply, 
        RequestVoteArgs, 
        RequestVoteReply
    }, OneTx, UbTx
};

pub enum Event {
    GetState(OneTx<(usize, bool)>),
    AppendEntries {
        args: AppendEntriesArgs,
        reply_tx: OneTx<AppendEntriesReply>,
    },
    RequestVote {
        args: RequestVoteArgs,
        reply_tx: OneTx<RequestVoteReply>
    },
    HeartBeatTimeout,
    Trans(Trans),
    // candidate related events
    GrantVote {
        term: usize,
        voter: usize,
    },
    
    OutdateCandidate {
        new_term: usize
    },

    ElectionTimeout
}

pub struct EvQueue {
    ev_ch: RwLock<UbTx<Event>>,
    key: AtomicUsize
}

impl EvQueue {
    pub fn new(ev_ch: UbTx<Event>) -> Self {
        Self {
            ev_ch: RwLock::new(ev_ch),
            key: AtomicUsize::new(0)
        }
    }

    /// put without pass key, only event from outside can 
    /// be put in this way, like Event::GetState.
    pub async fn just_put(&self, ev: Event) {
        let ev_ch = self.ev_ch.read().await;
        ev_ch.send(ev).unwrap();
    }

    /// Put an event to the event queue, return Err(event) 
    /// if not success.
    pub async fn put(&self, ev: Event, key: usize) -> Result<(), Event> {
        // reject events with unmatched key
        if self.key.load(Ordering::Acquire) != key {
            return Err(ev);
        }

        match ev {
            Event::Trans(to) => {
                let ev_ch = self.ev_ch.write().await;
                ev_ch.send(Event::Trans(to)).unwrap();
                self.key.fetch_add(1, Ordering::AcqRel);
            },
            ev => {
                let ev_ch = self.ev_ch.read().await;
                ev_ch.send(ev).unwrap();
            }
        }
        Ok(())
    }

    pub fn key(&self) -> usize {
        self.key.load(Ordering::Acquire)
    }

    // Try put an event for multiple times, 
    // return Err(()) in case of multiple failures.
    // pub async fn must_put(&self, mut ev: Event) -> Result<(), ()> {
    //     const WAIT: Duration = Duration::from_millis(20);
    //     const TRIES: usize = 10;
    //     for _ in 0..TRIES {
    //         ev = match self.put(ev).await {
    //             Ok(_) => return Ok(()),
    //             Err(ev) => ev
    //         };
    //         tokio::time::sleep(WAIT).await;
    //     }
    //     Err(())
    // }
}
