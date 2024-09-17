// Date:   Thu Sep 05 14:41:16 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{ops::Range, time::Duration};
use labrpc::{client::ClientEnd, err::{DISCONNECTED, TIMEOUT}};
use rand::Rng;
use serde::{de::DeserializeOwned, Serialize};

pub const HEARTBEAT_TIMEOUT: Range<u64> = 300..600;
pub const ELECTION_TIMEOUT: Range<u64> = 900..1100;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
pub const RPC_RETRY_WAIT: Duration = Duration::from_millis(20);
pub const DISCONNECT_RETRY: Duration = Duration::from_millis(200);
pub const NET_FAIL_WAIT: Duration = Duration::from_millis(100);

pub const REQUEST_VOTE: &'static str = "RpcService.request_vote";
pub const APPEND_ENTRIES: &'static str = "RpcService.append_entries";
pub const QUERY_ENTRY: &'static str = "RpcService.query_entry";

pub const RPC_FAIL_RETRY: usize = 5;

pub fn gen_rand_duration(range: Range<u64>) -> Duration {
    let ms: u64 = rand::thread_rng().gen_range(range);
    Duration::from_millis(ms)
}

// RpcRetry try the rpc call for multiple times, 
// return an Option instead of a Result.
#[async_trait::async_trait]
pub trait RpcRetry {
    async fn try_call<A, R, E>(&self, method: &str, arg: &A) -> Option<R> 
        where A: Serialize + Send + Sync,
        R: DeserializeOwned + Send + Sync,
        E: DeserializeOwned + Send + Sync;
}
#[async_trait::async_trait]
impl RpcRetry for ClientEnd {
    async fn try_call<A, R, E>(&self, method: &str, arg: &A) -> Option<R>
        where A: Serialize + Send + Sync,
        R: DeserializeOwned + Send + Sync,
        E: DeserializeOwned + Send + Sync
    {
        for _ in 0..RPC_FAIL_RETRY {
            let reply = self.call::<A, Result<R, E>>(method, arg).await;
            match reply {
                Ok(Ok(v)) => return Some(v),
                Ok(_) => return None,
                Err(TIMEOUT) => {
                    tokio::time::sleep(RPC_RETRY_WAIT).await;
                    continue;
                },
                Err(DISCONNECTED) => return None,
                Err(e) => panic!("Unexpected error: {e:?}")
            }
        }
        None
    }
}

#[macro_export]
#[cfg(not(feature = "no_debug"))]
macro_rules! log {
    (($($rgb: literal),*), $($args: expr),*) => {{
        use colored::Colorize;
        let msg = format!($($args),*).truecolor($($rgb),*);
        println!("{msg}");
    }}
}

#[cfg(feature = "no_debug")]
macro_rules! log {
    ($color: indent, $($args: expr),*) => {}
}

#[macro_export]
#[cfg(feature = "info")]
macro_rules! info {
    ($($args: expr),*) => {
        crate::log!((145, 178, 244), $($args),*)
    }
}
#[macro_export]
#[cfg(not(feature = "info"))]
macro_rules! info {
    ($($args: expr),*) => {}
}

#[macro_export]
macro_rules! warn {
    ($($args: expr),*) => {
        crate::log!((255,175,0), $($args),*)
    }
}
#[macro_export]
#[cfg(not(feature = "warn"))]
macro_rules! warn {
    ($($args: expr),*) => {}
}

#[macro_export]
macro_rules! error {
    ($($args: expr),*) => {
        crate::log!((255,0,0), $($args),*)
    }
}
#[macro_export]
#[cfg(not(feature = "error"))]
macro_rules! error {
    ($($args: expr),*) => {}
}
