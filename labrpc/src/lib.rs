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

pub use service::{Service, CallResult};

#[cfg(test)]
mod tests {
    use labrpc_macros::rpc;
    use crate::{Service, CallResult, err::{Error, ServiceError}};

    struct Hello;

    #[rpc(Service, CallResult, ServiceError)]
    impl Hello {
        pub fn hello(&self, name: String) -> String {
            format!("Hello, {name}")
        }
    }

    #[tokio::test]
    async fn unicast_test() {
        let network = crate::network::Network::new();
        let mut node1 = crate::end::End::new(&network);
        let node2 = crate::end::End::new(&network);

        node1.add_service("Hello".to_string(), Box::new(Hello)).await;
        
        assert_eq!(
            Ok("Hello, Jack".to_string()),
            node2.unicast::<_, String>(0, "Hello.hello", "Jack").await
        );

        assert_eq!(
            Err(Error::ServiceError(ServiceError::ClassNotFound)),
            node2.unicast::<_, String>(0, "WTF.hello", "Jack").await
        );

        assert_eq!(
            Err(Error::ServiceError(ServiceError::MethodNotFound)),
            node2.unicast::<_, String>(1, "Hello.wtf", "Jack").await
        );
    }

    #[tokio::test]
    async fn broadcast_test() {
        let network = crate::network::Network::new();
        let create_one = || async {
            let mut node = crate::end::End::new(&network);
            node.add_service("Hello".to_string(), Box::new(Hello)).await;
            node
        };
        
        let _node1 = create_one().await;
        let _node2 = create_one().await;
        let _node3 = create_one().await;
        let node4 = create_one().await;

        let collect_rx = |mut rx: crate::Rx<Result<String, ServiceError>>| async move {
            let mut bowl = Vec::new();
            while let Some(msg) = rx.recv().await {
                bowl.push(msg);
            }
            bowl
        };

        let rx = node4.broadcast("Hello.hello", "Jack".to_string()).await.unwrap();
        assert!(
            collect_rx(rx).await.into_iter()
            .map(|res| res.map_err(Error::from))
            .all(|res| res == Ok("Hello, Jack".to_string()))
        )
    }
}
