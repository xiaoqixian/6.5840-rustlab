// Date:   Tue Aug 20 17:53:52 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use super::rpc;

#[test]
fn test() {

trait Service {
    async fn call(&self, method: &str, arg: &[u8]) -> Vec<u8>;
}

struct Hello;

#[rpc]
impl Hello {
    pub fn hello(&self, name: String) -> String {
        format!("Hello, {name}")
    }
}

}
