// Date:   Mon Sep 30 22:49:17 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, Ordering}, Arc};

use bit_vec::BitVec;

use crate::{debug, event::Event, info, role::RoleEvQueue, warn, UbRx, UbTx};

struct CounterImpl {
    me: usize,
    n: usize,
    quorum: usize,
    offset: usize,
    indices: Vec<BitVec>,
    ev_q: RoleEvQueue,
    active: Arc<AtomicBool>
}

enum ReplCounterEv {
    WatchIdx(usize),
    Confirm {
        peer_id: usize,
        index: usize
    }
}

#[derive(Clone)]
pub struct ReplCounter {
    me: usize,
    tx: UbTx<ReplCounterEv>,
}

impl ReplCounter {
    pub fn new(me: usize, n: usize, lci: usize, ev_q: RoleEvQueue, active: Arc<AtomicBool>) -> Self {
        let counter = CounterImpl {
            me,
            n,
            quorum: n/2 + 1,
            // the first log is the noop log that is held by all nodes, 
            // so the first node is considered committed by all.
            offset: lci + 1,
            indices: Vec::new(),
            ev_q,
            active
        };
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(counter.start(rx));
        Self { me, tx }
    }

    pub fn watch_idx(&self, index: usize) {
        info!("{self}: Watch index {index}");
        if let Err(_) = self.tx.send(ReplCounterEv::WatchIdx(index)) {
            warn!("{self}: channel closed");
        }
    }

    pub fn confirm(&self, peer_id: usize, index: usize) {
        info!("{self}: {peer_id} confirmed until {index}");
        let ev = ReplCounterEv::Confirm { peer_id, index };
        if let Err(_) = self.tx.send(ev) {
            warn!("{self}: channel closed");
        }
    }
}

impl CounterImpl {
    async fn start(mut self, mut rx: UbRx<ReplCounterEv>) {
        while let Some(ev) = rx.recv().await {
            match ev {
                ReplCounterEv::WatchIdx(idx) => self.watch_idx(idx),
                ReplCounterEv::Confirm { peer_id, index } => 
                    self.confirm(peer_id, index)
            }

            if !self.active.load(Ordering::Relaxed) {
                rx.close();
                break;
            }
        }
    }

    fn watch_idx(&mut self, index: usize) {
        debug_assert_eq!(index, self.indices.len() + self.offset);
        debug_assert!(self.n > 0);
        let mut bitvec = BitVec::from_elem(self.n, false);
        bitvec.set(self.me, true);
        self.indices.push(bitvec);
    }

    /// Peer confirms logs until index
    fn confirm(&mut self, peer_id: usize, index: usize) {
        if index < self.offset {
            return;
        }
        // debug_assert!(self.indices.len() >= range.len(), 
        //     "Unexpected confirm range, expect {:?}, got {range:?}", 
        //     Range {start: self.offset, end: self.offset + self.indices.len()});

        let mut new_offset = self.offset;

        for idx in self.offset..=index {
            let bitvec = &mut self.indices[idx - self.offset];
            bitvec.set(peer_id, true);
            if new_offset == idx && bitvec.count_ones() as usize >= self.quorum {
                new_offset = idx + 1;
            }
        }

        if new_offset != self.offset {
            debug!("Counter[{}]: update offset {} -> {new_offset}", self.me, self.offset);
            let _ = self.ev_q.put(Event::UpdateCommit(new_offset-1));
            let new_start = new_offset - self.offset;
            self.offset = new_offset;
            self.indices = self.indices.drain(new_start..).collect();
        }
    }
}

impl std::fmt::Display for CounterImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RelCounterImpl [{}]", self.me)
    }
}

impl std::fmt::Display for ReplCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RelCounter [{}]", self.me)
    }
}
