// Date:   Thu Sep 05 14:41:16 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{ops::Range, time::Duration};
use rand::Rng;

pub const HEARTBEAT_TIMEOTU: Range<u64> = 300..400;

pub fn gen_rand_duration(range: Range<u64>) -> Duration {
    let ms: u64 = rand::thread_rng().gen_range(range);
    Duration::from_millis(ms)
}
