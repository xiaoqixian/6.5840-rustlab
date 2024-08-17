// Date:   Fri Aug 16 15:57:36 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

pub enum MsgType {
    Unicast(u32),
    Broadcast
}

type ReplyTx = tokio::sync::oneshot::Sender<Vec<u8>>;

pub struct Msg {
    pub msg_type: MsgType,
    pub from: u32,
    pub cls: String,
    pub method: String,
    pub arg: Vec<u8>,
    pub reply_ch: ReplyTx
}
