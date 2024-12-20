// Date:   Thu Aug 29 17:01:15 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Storage {
    raft_state: Option<Vec<u8>>,
    snapshot: Option<Vec<u8>>,
}

/// If storage.is_none(), it means the Persister
/// is no longer available for this instance.
/// Hence the read and write should return a None
/// to represent the persistence failed.
#[derive(Clone)]
pub struct Persister {
    storage: Arc<Mutex<Option<Storage>>>,
}

impl Persister {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(Some(Storage::default()))),
        }
    }

    /// overwrite represents should the states should always be replaced
    /// even the args provided are None.
    pub fn save(
        &self,
        raft_state: Option<Vec<u8>>,
        snapshot: Option<Vec<u8>>,
        overwrite: bool,
    ) -> bool {
        let mut guard = self.storage.lock().unwrap();
        let storage = match guard.as_mut() {
            None => return false,
            Some(s) => s,
        };

        if raft_state.is_some() || overwrite {
            storage.raft_state = raft_state;
        }
        if snapshot.is_some() || overwrite {
            storage.snapshot = snapshot;
        }
        true
    }
}

#[cfg(test)]
impl Persister {
    pub fn raft_state_size(&self) -> usize {
        self.storage
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .raft_state
            .as_ref()
            .map(|s| s.len())
            .or(Some(0))
            .unwrap()
    }
}

/// This function is provided for the tester.
/// You are not supposed to call it.
#[cfg(test)] 
use crate::tests::Result;
#[cfg(test)]
pub fn make_persister(
    persister: Persister,
) -> Result<(Persister, Option<Vec<u8>>, Option<Vec<u8>>)> {
    use std::time::Duration;

    use crate::fatal;

    let mut tries = 0;
    let mut guard = loop {
        match persister.storage.try_lock() {
            Ok(g) => break g,
            Err(_) => {
                if tries < 10 {
                    tries += 1;
                    std::thread::sleep(Duration::from_millis(100));
                } else {
                    fatal!("Unable to lock Persister for a long time, \
                        expect no longer than 1 sec");
                }
            }
        }
    };
    let storage = guard.take().unwrap();
    let raft_state = storage.raft_state.clone();
    let snapshot = storage.snapshot.clone();
    let new = Persister {
        storage: Arc::new(Mutex::new(Some(storage))),
    };
    Ok((new, raft_state, snapshot))
}
