// Date:   Thu Aug 15 13:18:58 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod network;
pub mod msg;
pub mod service;
pub mod err;
pub mod client;
pub mod server;

use tokio::sync::mpsc as tk_mpsc;

type Rx<T> = tk_mpsc::Receiver<T>;
type UbRx<T> = tk_mpsc::UnboundedReceiver<T>;
type UbTx<T> = tk_mpsc::UnboundedSender<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;

pub use service::{Service, CallResult};

#[cfg(test)]
mod tests {
    use labrpc_macros::rpc;
    use crate::{err::{self, PEER_NOT_FOUND, TIMEOUT}, service::CallResult, Service};

    struct Hello;

    #[rpc(Service, CallResult, err)]
    impl Hello {
        pub fn hello(&self, name: String) -> String {
            format!("Hello, {name}")
        }
    }

    #[tokio::test]
    async fn network_test() {
        let mut network = crate::network::Network::new();

        let mut clients = Vec::with_capacity(5);
        for _ in 0..5 {
            let mut c = network.join_one().await;
            c.add_service("Hello".to_string(), Box::new(Hello)).await;
            clients.push(c);
        }

        let client0 = &clients[0];

        assert_eq!(
            Ok("Hello, Lunar".to_string()),
            client0.unicast::<_, String>(1, "Hello.hello", "Lunar".to_string()).await
        );

        // send to myself should return PEER_NOT_FOUND error
        assert_eq!(
            Err(crate::err::PEER_NOT_FOUND),
            client0.unicast::<_, String>(5, "Hello.hello", "Lunar".to_string()).await
        );

        // send to a non-exist node should return PEER_NOT_FOUND error
        assert_eq!(
            Err(crate::err::PEER_NOT_FOUND),
            client0.unicast::<_, String>(5, "Hello.hello", "Lunar".to_string()).await
        );

        assert_eq!(
            Err(crate::err::CLASS_NOT_FOUND),
            client0.unicast::<_, String>(1, "WTF.hello", "Lunar".to_string()).await
        );
        assert_eq!(
            Err(crate::err::METHOD_NOT_FOUND),
            client0.unicast::<_, String>(1, "Hello.wtf", "Lunar".to_string()).await
        );
        assert_eq!(
            Err(crate::err::INVALID_ARGUMENT),
            client0.unicast::<_, String>(1, "Hello.hello", 12).await
        );

        let mut rx = client0.broadcast::<_, String>(
            "Hello.hello", "Lunar".to_string()).await.unwrap();
        let mut rets = Vec::new();
        let get = rx.recv_many(&mut rets, 4).await;
        assert_eq!(get, 4);
        assert!(
            rets.into_iter()
                .all(|r| r == Ok("Hello, Lunar".to_string()))
        );
    }

    #[tokio::test]
    async fn net_change_test() {
        let mut network = crate::network::Network::new();

        let mut clients = Vec::with_capacity(5);
        for _ in 0..5 {
            let mut c = network.join_one().await;
            c.add_service("Hello".to_string(), Box::new(Hello)).await;
            clients.push(c);
        }

        let client0 = &clients[0];

        network.disconnect(2).await;
        assert_eq!(
            Ok("Hello, Lunar".to_string()),
            client0.unicast::<_, String>(1, "Hello.hello", "Lunar".to_string()).await
        );
        assert_eq!(
            Err(TIMEOUT),
            client0.unicast::<_, String>(2, "Hello.hello", "Lunar".to_string()).await
        );

        network.connect(2).await;
        assert_eq!(
            Ok("Hello, Lunar".to_string()),
            client0.unicast::<_, String>(2, "Hello.hello", "Lunar".to_string()).await
        );

        network.delete_server(2).await;
        assert_eq!(
            Err(PEER_NOT_FOUND),
            client0.unicast::<_, String>(2, "Hello.hello", "Lunar".to_string()).await
        );

        network.join_at(2).await;
    }
}
