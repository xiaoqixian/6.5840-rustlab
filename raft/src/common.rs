// Date:   Thu Sep 05 14:41:16 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{ops::Range, time::Duration};
use rand::Rng;

pub const HEARTBEAT_TIMEOUT: Range<u64> = 300..400;

pub const REQUEST_VOTE: &'static str = "RpcService.request_vote";
pub const APPEND_ENTRIES: &'static str = "RpcService.append_entries";

pub fn gen_rand_duration(range: Range<u64>) -> Duration {
    let ms: u64 = rand::thread_rng().gen_range(range);
    Duration::from_millis(ms)
}
