// Date:   Wed May 29 21:27:58 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{collections::HashMap, sync::{Arc, RwLock}};

use super::{Reply, ReplyChOp};
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};

pub struct ReplyReceiver<R> {
    req_id: u64,
    rx: UnboundedReceiver<Vec<u8>>,
    reply_op_tx: Arc<UnboundedSender<ReplyChOp>>,
    _marker: std::marker::PhantomData<R>
}

impl<R: DeserializeOwned> ReplyReceiver<R> {
    pub fn new(
        req_id: u64,
        rx: UnboundedReceiver<Vec<u8>>,
        reply_op_tx: Arc<UnboundedSender<ReplyChOp>>,
    ) -> Self {
        Self {
            req_id,
            rx,
            reply_op_tx,
            _marker: std::marker::PhantomData
        }
    }

    pub async fn recv(&mut self) -> Option<Reply<R>> {
        self.rx.recv().await.map(From::from)
    }
}

impl<R> Drop for ReplyReceiver<R> {
    fn drop(&mut self) {
        self.reply_op_tx.send(ReplyChOp::RemoveCh(self.req_id)).unwrap();
    }
}
