// Date:   Tue Aug 20 17:36:35 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

type Res = Result<Vec<u8>, Error>;

#[async_trait::async_trait]
trait Service {
    async fn call(&self, meth: &str, arg: &[u8]) -> Res;
}

enum Error {
    MethodNotFound,
    InvalidArgument
}

struct Hello;

use labrpc_macros::rpc;

#[rpc(Service, Res, Error)]
impl Hello {
    pub fn hello(&self, name: String) -> String {
        format!("Hello, {name}")
    }
}

fn main() {}
