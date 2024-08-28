// Date:   Fri Aug 16 15:57:36 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{service::CallResult, OneTx};

#[derive(Clone)]
pub struct RpcReq {
    pub cls: String,
    pub method: String,
    pub arg: Vec<u8>,
}

pub struct Msg {
    pub end_id: u32,
    pub req: RpcReq,
    pub reply_tx: OneTx<CallResult>
}
