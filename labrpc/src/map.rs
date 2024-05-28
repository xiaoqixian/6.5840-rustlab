// Date:   Mon May 27 15:12:16 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use tokio::sync::mpsc::UnboundedReceiver;

pub struct MapRx<T, U> {
    rx: UnboundedReceiver<T>,
    _maker: std::marker::PhantomData<U>
}

impl<T, U: From<T>> MapRx<T, U> {
    pub fn new(rx: UnboundedReceiver<T>) -> Self {
        Self {
            rx,
            _maker: std::marker::PhantomData,
        }
    }

    pub async fn recv(&mut self) -> Option<U> {
        self.rx.recv().await.map(|t| {
            U::from(t)
        })
    }
}
