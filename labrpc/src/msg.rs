// Date:   Fri Aug 16 15:57:36 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{err::{Error, NetworkError}, service::CallResult};

type OneshotSender<T> = tokio::sync::oneshot::Sender<T>;
type MultishotSender<T> = tokio::sync::mpsc::Sender<T>;

pub enum ReplyTx<T> {
    Oneshot(OneshotSender<T>),
    Multishot(MultishotSender<T>)
}

#[derive(Clone)]
pub struct RpcReq {
    pub cls: String,
    pub method: String,
    pub arg: Vec<u8>,
}

pub struct Pack {
    pub req: RpcReq,
    pub reply_tx: ReplyTx<CallResult>
}

pub struct Msg {
    pub from: u32,
    pub to: u32,
    pub pack: Pack
}

impl<T> ReplyTx<T> {
    /// No matter the inner channel is oneshot or multipleshot, 
    /// they are both designed to be oneshot, so the object
    /// is consumed.
    pub async fn send(self, result: T) -> Result<(), Error> {
        match self {
            Self::Oneshot(oneshot) => {
                oneshot.send(result)
                    .map_err(|_| Error::NetworkError(
                        NetworkError::ChannelError(
                            format!("Reply oneshot send error")
                        )
                    ))
            }
            Self::Multishot(multi) => {
                multi.send(result).await
                    .map_err(|err| Error::from(err))
            }
        }
    }
}

impl<T> From<OneshotSender<T>> for ReplyTx<T> {
    fn from(value: OneshotSender<T>) -> Self {
        Self::Oneshot(value)
    }
}

impl<T> From<MultishotSender<T>> for ReplyTx<T> {
    fn from(value: MultishotSender<T>) -> Self {
        Self::Multishot(value)
    }
}
