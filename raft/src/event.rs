// Date:   Thu Sep 05 16:01:48 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{atomic::{AtomicBool, Ordering}, Arc};

use tokio::sync::RwLock;

use crate::{OneTx, UbTx};

pub enum Event {
    GetState(OneTx<(usize, bool)>),
    HeartBeatTimeout,
    Trans
}

#[derive(Clone)]
pub struct EvQueue {
    ev_ch: Arc<RwLock<UbTx<Event>>>,
    locked: Arc<AtomicBool>
}

impl EvQueue {
    pub fn new(ev_ch: UbTx<Event>) -> Self {
        Self {
            ev_ch: Arc::new(RwLock::new(ev_ch)),
            locked: Default::default()
        }
    }

    /// Put an event to the event queue, return Err(event) 
    /// if not success.
    pub async fn put(&self, ev: Event) -> Result<(), Event> {
        let is_trans = match &ev {
            Event::Trans => true,
            _ => false
        };

        if is_trans {
            let ev_ch = self.ev_ch.write().await;
            ev_ch.send(ev).unwrap();
            let locked = self.locked.swap(true, Ordering::AcqRel);
            assert!(!locked);
            Ok(())
        } else {
            let ev_ch = self.ev_ch.read().await;
            if self.locked.load(Ordering::Acquire) {
                return Err(ev);
            }
            ev_ch.send(ev).unwrap();
            Ok(())
        }
    }
}
