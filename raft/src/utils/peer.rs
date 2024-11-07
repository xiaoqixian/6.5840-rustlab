// Date:   Tue Oct 01 14:56:23 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::Duration};

use labrpc::{client::ClientEnd, err::{DISCONNECTED, TIMEOUT}};
use serde::{de::DeserializeOwned, Serialize};

use crate::common::{DISCONNECT_WAIT, RPC_RETRY_WAIT};

/// A wrapped ClientEnd to make dialing process simpler.
pub struct Peer {
    end: ClientEnd,
    active: Arc<AtomicBool>
}


impl Peer {
    pub fn new(end: ClientEnd, active: Arc<AtomicBool>) -> Self {
        Self { end, active }
    }

    pub fn to(&self) -> usize {
        self.end.to()
    }

    /// Retry on timeout for multiple times
    pub async fn try_call<A, R, E>(&self, method: &str, arg: &A, tries: usize) -> Option<R>
        where A: Serialize + Send + Sync,
              R: DeserializeOwned + Send + Sync,
              E: DeserializeOwned + Send + Sync
    {
        for _ in 0..tries {
            if !self.active.load(Ordering::Relaxed) { break; }

            let call = self.end.call::<A, Result<R, E>>(method, arg);
            let reply = match tokio::time::timeout(
                RPC_RETRY_WAIT,
                call
            ).await {
                Ok(r) => r,
                Err(_) => Err(TIMEOUT)
            };
            match reply {
                Ok(Ok(v)) => return Some(v),
                Ok(_) => return None,
                Err(TIMEOUT) => {
                    // tokio::time::sleep(RPC_RETRY_WAIT).await;
                    continue;
                },
                Err(DISCONNECTED) => return None,
                Err(e) => panic!("Unexpected error: {e:?}")
            }
        }
        None
    }

    /// Keep calling on TIMEOUT and DISCONNECTED situations.
    pub async fn call<A, R, E>(&self, method: &str, arg: &A) -> Option<R>
        where A: Serialize + Send + Sync,
              R: DeserializeOwned + Send + Sync,
              E: DeserializeOwned + Send + Sync
    {
        while self.active.load(Ordering::Relaxed) {
            let call = self.end.call::<A, Result<R, E>>(method, arg);
            let reply = match tokio::time::timeout(
                RPC_RETRY_WAIT,
                call
            ).await {
                Ok(r) => r,
                Err(_) => Err(TIMEOUT)
            };
            match reply {
                Ok(Ok(v)) => return Some(v),
                Err(DISCONNECTED) => {
                    tokio::time::sleep(DISCONNECT_WAIT).await;
                },
                Ok(_) | Err(TIMEOUT) => {
                    // tokio::time::sleep(RPC_RETRY_WAIT).await;
                },
                Err(e) => panic!("Unexpected error: {e:?}")
            }
        }
        None
    }
}
