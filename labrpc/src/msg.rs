// Date:   Fri Aug 16 15:57:36 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{service::CallResult, OneTx};

#[derive(Clone)]
pub(crate) struct RpcReq {
    pub cls: String,
    pub method: String,
    pub arg: Vec<u8>,
}

pub(crate) struct Msg {
    pub end_id: usize,
    pub req: RpcReq,
    pub reply_tx: OneTx<CallResult>
}
