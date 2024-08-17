// Date:   Fri Aug 16 15:36:01 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::collections::HashMap;
use tokio::sync::mpsc as tk_mpsc;

use super::{Tx, Rx};

pub struct Network {
    tx: Tx,
    ends: HashMap<u32, Tx>
}

impl Network {
    pub fn new() -> Self {
        let (tx, rx) = tk_mpsc::unbounded_channel();
        tokio::spawn(Self::run(rx));
        Self {
            tx,
            ends: HashMap::new()
        }
    }

    async fn run(mut rx: Rx) {
        while let Some(_msg) = rx.recv().await {}
    }

    pub fn join(&mut self, clt_tx: Tx) -> (u32, Tx) {
        let id = self.ends.len() as u32;
        self.ends.insert(id, clt_tx);
        (id, self.tx.clone())
    }
}
