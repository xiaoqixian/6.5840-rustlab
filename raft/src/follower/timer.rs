// Date:   Thu Oct 03 19:50:56 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::time::Duration;

use crate::UbTx;

/// A restettable timer
pub struct Timer {
    tx: UbTx<()>,
}

impl Timer {
    pub fn new<F, G>(action: F, dur_gen: G) -> Self
    where
        F: Send + 'static + FnOnce(),
        G: Send + 'static + Fn() -> Duration,
    {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            loop {
                let d = dur_gen();
                tokio::select! {
                    _ = tokio::time::sleep(d) => {
                        action();
                        break;
                    },
                    v = rx.recv() => {
                        if v.is_none() {
                            break;
                        }
                    }
                }
            }
        });
        Self { tx }
    }

    pub fn reset(&self) -> bool {
        self.tx.send(()).is_ok()
    }
}
