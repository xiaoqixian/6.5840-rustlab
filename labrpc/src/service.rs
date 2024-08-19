// Date:   Fri Aug 16 16:47:42 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::err::ServiceError;

pub type CallResult = Result<Vec<u8>, ServiceError>;

pub trait Service: Send + Sync {
    fn call(&self, method: &str, arg: &[u8]) -> CallResult;
}
