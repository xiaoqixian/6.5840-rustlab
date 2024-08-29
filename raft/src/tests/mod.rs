// Date:   Thu Aug 29 16:27:57 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

#[cfg(test)]
mod test_3a;

use std::collections::HashMap;

use labrpc::network::Network;

struct Config {
    net: Network,
    bytes: usize,
    last_applied: Vec<usize>,
    logs: Vec<HashMap<usize, Vec<u8>>>
}

impl Config {
    pub async fn new(n: usize, unreliable: bool, snapshot: bool) -> Self {
        let net = Network::new();
    }
}
