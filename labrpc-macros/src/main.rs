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

mod err {
    use super::Error;
    pub const METHOD_NOT_FOUND: Error = Error::MethodNotFound;
    pub const INVALID_ARGUMENT: Error = Error::InvalidArgument;
}

struct Hello<T>(T);

use labrpc_macros::rpc;

trait M {}

// #[async_trait::async_trait]
// impl<T> Service for Hello<T> 
// where T: M + Send + Sync
// {
//     async fn call(&self, meth: &str, arg: &[u8]) -> Res {
//         Err(err::METHOD_NOT_FOUND)
//     }
// }

#[rpc(Service, Res, err)]
impl<T> Hello<T> where T: M + Send + Sync {
    pub fn hello(&self, name: String) -> String {
        format!("Hello, {name}")
    }
}

fn main() {}
