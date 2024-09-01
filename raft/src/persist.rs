// Date:   Thu Aug 29 17:01:15 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use tokio::sync::RwLock;

#[derive(Clone, Default)]
struct Store {
    raft_state: Option<Vec<u8>>,
    snapshot: Option<Vec<u8>>
}

#[derive(Clone, Default)]
pub struct Persister(Arc<RwLock<Store>>);

impl Persister {
    pub fn new() -> Self {
        Self(Default::default())
    }

    pub async fn clone(&self) -> Self {
        let store = self.0.read().await;
        Self(Arc::new(RwLock::new(store.clone())))
    }

    pub async fn raft_state(&self) -> Option<Vec<u8>> {
        let store = self.0.read().await;
        store.raft_state.clone()
    }

    pub async fn snapshot(&self) -> Option<Vec<u8>> {
        let store = self.0.read().await;
        store.snapshot.clone()
    }

    pub async fn save(&self, raft_state: Option<Vec<u8>>, 
        snapshot: Option<Vec<u8>>) {
        let mut store = self.0.write().await;
        *store = Store { raft_state, snapshot };
    }
}

