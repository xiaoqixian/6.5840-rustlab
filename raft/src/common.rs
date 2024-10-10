// Date:   Thu Sep 05 14:41:16 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{ops::Range, time::Duration};
use rand::Rng;

pub const HEARTBEAT_TIMEOUT: Range<u64> = 300..400;
pub const ELECTION_TIMEOUT: Range<u64> = 900..1100;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
pub const RPC_RETRY_WAIT: Duration = Duration::from_millis(100);
pub const DISCONNECT_WAIT: Duration = Duration::from_millis(200);
pub const NET_FAIL_WAIT: Duration = Duration::from_millis(100);

pub const REQUEST_VOTE: &'static str = "RpcService.request_vote";
pub const APPEND_ENTRIES: &'static str = "RpcService.append_entries";
pub const QUERY_ENTRY: &'static str = "RpcService.query_entry";

pub const RPC_FAIL_RETRY: usize = 5;

pub fn gen_rand_duration(range: Range<u64>) -> Duration {
    let ms: u64 = rand::thread_rng().gen_range(range);
    Duration::from_millis(ms)
}


#[macro_export]
macro_rules! log {
    (($($rgb: literal),*), $($args: expr),*) => {
        #[cfg(not(feature = "no_debug"))]
        {
            use colored::Colorize;
            let msg = format!($($args),*).truecolor($($rgb),*);
            println!("{msg}");
        }
    }
}

#[macro_export]
macro_rules! info {
    ($($args: expr),*) => {
        #[cfg(feature = "info_log_level")]
        {
            crate::log!((145, 178, 244), $($args),*)
        }
    }
}

#[macro_export]
macro_rules! warn {
    ($($args: expr),*) => {
        #[cfg(debug_assertions)]
        {
            use colored::Colorize;
            let msg = format!($($args),*).truecolor(255,175,0);
            eprintln!("{msg}");
            // crate::log!((255,175,0), $($args),*)
        }
    }
}

#[macro_export]
macro_rules! error {
    ($($args: expr),*) => {
        #[cfg(feature = "error_log_level")]
        {
            crate::log!((255,0,0), $($args),*)
        }
    }
}

#[macro_export]
macro_rules! debug {
    ($($args: expr),*) => {
        #[cfg(not(feature = "no_debug"))]
        {
            use colored::Colorize;
            let msg = format!("[DEBUG] {}", format_args!($($args), *))
                .yellow();
            println!("{msg}");
        }
    }
}
