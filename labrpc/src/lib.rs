// Date:   Thu Aug 15 13:18:58 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod network;
pub mod err;
pub mod client;
pub mod server;

use tokio::sync::mpsc as tk_mpsc;

type Rx<T> = tk_mpsc::Receiver<T>;
type UbRx<T> = tk_mpsc::UnboundedReceiver<T>;
type UbTx<T> = tk_mpsc::UnboundedSender<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;

pub type CallResult = Result<Vec<u8>, err::Error>;

#[async_trait::async_trait]
pub trait Service: Send + Sync {
    async fn call(&self, method: &str, arg: &[u8]) -> CallResult;
}

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use labrpc_macros::rpc;
    use crate::{err::{self, DISCONNECTED, TIMEOUT}, CallResult, Service};

    struct Hello;

    #[rpc(Service, CallResult, err)]
    impl Hello {
        pub fn hello(&self, name: String) -> String {
            format!("Hello, {name}")
        }

        pub async fn no_response(&self, _: ()) {
            // sleep a long time to simulate the situation where
            // the service is unresponsive.
            tokio::time::sleep(Duration::from_secs(120)).await;
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

        let (mut rx, _) = client0.broadcast::<_, String>(
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
        // server[2] should be disabled.
        assert_eq!(
            Err(TIMEOUT),
            client0.unicast::<_, String>(2, "Hello.hello", "Lunar".to_string()).await
        );
        // client[2] should be disabled too.
        assert_eq!(
            Err(DISCONNECTED),
            clients[2].unicast::<_, String>(1, "Hello.hello", "Lunar".to_string()).await
        );

        network.connect(2).await;
        assert_eq!(
            Ok("Hello, Lunar".to_string()),
            client0.unicast::<_, String>(2, "Hello.hello", "Lunar".to_string()).await
        );
        assert_eq!(
            Ok("Hello, Lunar".to_string()),
            clients[2].unicast::<_, String>(1, "Hello.hello", "Lunar".to_string()).await
        );
    }

    /// When a service is not responsing for a long time, 
    /// the network should be able to return a TIMEOUT error 
    /// to the caller.
    /// The network waiting time is 20 secs.
    #[tokio::test]
    async fn no_response_test() {
        let mut network = crate::network::Network::new();
        const N: usize = 2;

        let mut clients = Vec::with_capacity(N);
        for _ in 0..N {
            let mut c = network.join_one().await;
            c.add_service("Hello".to_string(), Box::new(Hello)).await;
            clients.push(c);
        }
        
        let client0 = &clients[0];

        assert_eq!(
            Err(TIMEOUT),
            client0.unicast::<_, ()>(1, "Hello.no_response", ()).await
        );
    }
}
