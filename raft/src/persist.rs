// Date:   Thu Aug 29 17:01:15 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{Arc, Mutex};

use serde::Deserialize;

use crate::{logs::LogsInfo, raft::RaftInfo};

#[derive(Clone, Default)]
struct Storage {
    raft_state: Option<Vec<u8>>,
    snapshot: Option<Vec<u8>>
}

#[derive(Default)]
pub struct Persister {
    storage: Arc<Mutex<Option<Storage>>>,
}

#[derive(Default)]
pub struct PersisterManager {
    storage: Arc<Mutex<Option<Storage>>>,
}

impl Persister {
    pub fn raft_state(&self) -> Option<Vec<u8>> {
        let storage = self.storage.lock().unwrap();
        match storage.as_ref() {
            None => None,
            Some(s) => s.raft_state.clone()
        }
    }

    pub fn snapshot(&self) -> Option<Vec<u8>> {
        let storage = self.storage.lock().unwrap();
        match storage.as_ref() {
            None => None,
            Some(s) => s.snapshot.clone()
        }
    }

    pub fn save(
        &self, 
        raft_state: Option<Vec<u8>>, 
        snapshot: Option<Vec<u8>>
    ) -> bool {
        let mut guard = self.storage.lock().unwrap();
        let storage = match guard.as_mut() {
            None => return false,
            Some(s) => s
        };
        if let Some(raft_state) = raft_state {
            storage.raft_state = Some(raft_state);
        }
        if let Some(snapshot) = snapshot {
            storage.snapshot = Some(snapshot);
        }
        true
    }
}

impl PersisterManager {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(Some(Storage::default())))
        }
    }

    pub fn lock(&mut self) {
        let storage = self.storage.lock().unwrap().take().unwrap();
        self.storage = Arc::new(Mutex::new(Some(storage)));
    }

    /// make_new takes the storage away, so the old Persister will 
    /// fail on saving.
    pub fn make_new(&mut self) -> Persister {
        Persister {
            storage: self.storage.clone()
        }
    }

    pub fn snapshot(&self) -> Option<Vec<u8>> {
        let storage = self.storage.lock().unwrap();
        match storage.as_ref() {
            None => None,
            Some(s) => s.snapshot.clone()
        }
    }
}

// #[derive(Serialize)]
// pub struct PersistStateSe<'a, 'b> {
//     pub raft_info: &'a RaftInfo,
//     pub logs_info: &'b LogsInfo
// }
//
#[derive(Default, Deserialize)]
pub struct PersistStateDes {
    pub raft_info: RaftInfo,
    pub logs_info: LogsInfo
}

// impl<T> Persister for T 
// where T: AsRef<(RaftCore, Logs)> 

// pub struct RaftPersister {
//     buffer: Vec<u8>,
//     persister: Persister,
//     logs_info_offset: usize,
//     logs_offset: usize,
// }
//
// #[derive(Default)]
// pub struct PersistState {
//     pub raft_info: RaftInfo,
//     pub logs_info: LogsInfo,
//     pub logs: Vec<LogEntry>
// }
//
// struct BufWriter<'a> {
//     buffer: &'a mut Vec<u8>,
//     offset: usize
// }
//
// impl RaftPersister {
//     pub async fn new(persister: Persister) -> (Self, PersistState) {
//         let raft_info_len = bincode::serialized_size(&RaftInfo::default()).unwrap() as usize;
//         let logs_info_len = bincode::serialized_size(&LogsInfo::default()).unwrap() as usize;
//
//         let (buffer, persist_dec) = match persister.raft_state().await {
//             Some(buf) => {
//                 let mut base = 0usize;
//                 let raft_info = bincode::deserialize_from(&buf[..raft_info_len]).unwrap();
//                 base += raft_info_len;
//
//                 let logs_info = bincode::deserialize_from(&buf[base..base+logs_info_len]).unwrap();
//                 base += logs_info_len;
//
//                 let logs = bincode::deserialize_from(&buf[base..]).unwrap();
//                 
//                 (buf, PersistState {
//                     raft_info,
//                     logs_info,
//                     logs
//                 })
//             },
//             None => (
//                 Vec::with_capacity(raft_info_len + logs_info_len + 4096),
//                 PersistState::default()
//             )
//         };
//
//         (
//             Self {
//                 buffer,
//                 persister,
//                 logs_info_offset: raft_info_len,
//                 logs_offset: raft_info_len + logs_info_len,
//             },
//             persist_dec
//         )
//     }
//
//     /// Let's default the buffer size won't overflow.
//     fn buf_expand(&mut self) {
//         let mut new_buf = Vec::with_capacity(self.buffer.len() << 1);
//         new_buf.copy_from_slice(&self.buffer[..]);
//         self.buffer = new_buf;
//     }
//
//     pub async fn save_raft_info(&mut self, value: RaftInfo) {
//         {
//             let writer = BufWriter {
//                 buffer: &mut self.buffer,
//                 offset: 0
//             };
//             bincode::serialize_into(writer, &value).unwrap();
//         }
//         self.persister.save(Some(self.buffer.clone()), None).await;
//     }
//
//     pub async fn save_logs_info(&mut self, value: LogsInfo) {
//         {
//             let writer = BufWriter {
//                 buffer: &mut self.buffer,
//                 offset: self.logs_info_offset
//             };
//             bincode::serialize_into(writer, &value).unwrap();
//         }
//         self.persister.save(Some(self.buffer.clone()), None).await;
//     }
//
//     pub async fn save_entries(&mut self, entries: Vec<LogEntry>) {
//         {
//             let writer = BufWriter {
//                 buffer: &mut self.buffer,
//                 offset: self.logs_offset
//             };
//             bincode::serialize_into(writer, &entries).unwrap();
//         }
//         self.persister.save(Some(self.buffer.clone()), None).await;
//     }
// }
//
// impl<'a> std::io::Write for BufWriter<'a> {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//         let remain_len = self.buffer.len() - self.offset;
//         if remain_len < buf.len() {
//             return Ok(0);
//         }
//
//         let range = self.offset..(self.offset + buf.len());
//         self.buffer[range].copy_from_slice(buf);
//         Ok(buf.len())
//     }
//
//     fn flush(&mut self) -> std::io::Result<()> {
//         Ok(())
//     }
// }
//
