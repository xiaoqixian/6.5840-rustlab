// Date:   Tue Oct 01 16:05:46 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{warn, ApplyMsg, UbRx, UbTx};
pub enum ApplyEntry {
    Entries {
        entries: Vec<(usize, Vec<u8>)>
    },
    // Snapshot
}

pub struct Applier {
    apply_ch: UbTx<ApplyMsg>,
}

impl Applier {
    pub fn new(apply_ch: UbTx<ApplyMsg>) -> Self {
        Self {
            apply_ch,
        }
    }

    pub async fn start(self, mut rx: UbRx<ApplyEntry>) {
        while let Some(et) = rx.recv().await {
            match et {
                ApplyEntry::Entries { entries } => {
                    for (index, command) in entries.into_iter() {
                        let apply_msg = ApplyMsg::Command {
                            command, index
                        };
                        if let Err(_) = self.apply_ch.send(apply_msg) {
                            warn!("Apply channel is closed");
                        }
                    }
                }
            }
        }
    }
}
