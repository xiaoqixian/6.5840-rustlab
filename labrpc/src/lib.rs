// Date:   Thu Aug 15 13:18:58 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod network;
pub mod msg;
pub mod end;
pub mod service;
pub mod err;

use tokio::sync::mpsc as tk_mpsc;

type Rx<T> = tk_mpsc::Receiver<T>;
type UbRx<T> = tk_mpsc::UnboundedReceiver<T>;
type UbTx<T> = tk_mpsc::UnboundedSender<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;

pub use service::{Service, CallResult};

#[cfg(test)]
mod tests {
    use labrpc_macros::rpc;
    use crate::{end::End, err::{Error, ServiceError}, CallResult, Service};

    struct Hello;

    #[rpc(Service, CallResult, ServiceError)]
    impl Hello {
        pub fn hello(&self, name: String) -> String {
            format!("Hello, {name}")
        }
    }

    #[tokio::test]
    async fn network_test() {
        let network = crate::network::Network::new();
        let mut nodes = Vec::with_capacity(5);
        for _ in 0..5 {
            let mut node = End::new(&network).await;
            node.add_service("Hello".to_string(), Box::new(Hello)).await;
            nodes.push(node);
        }

        
    }

}
