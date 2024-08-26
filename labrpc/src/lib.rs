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
    use crate::{Service, service::CallResult, err::ServiceError};

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
        // let mut servers = Vec::<crate::end::Admin>::new();
        for _ in 0..5 {
            let mut admin = crate::end::Admin::new();
            admin.add_service("Hello".to_string(), Box::new(Hello)).await;
            admin.join(&network).await;
            // servers.push(admin);
        }

        let client0 = network.make_client_for(0).await;
        
        assert_eq!(
            Ok("Hello, Lunar".to_string()),
            client0.unicast::<_, String>(0, "Hello.hello", "Lunar".to_string()).await
        );

        assert_eq!(
            Err(crate::err::PEER_NOT_FOUND),
            client0.unicast::<_, String>(5, "Hello.hello", "Lunar".to_string()).await
        );

        assert_eq!(
            Err(crate::err::CLASS_NOT_FOUND),
            client0.unicast::<_, String>(0, "WTF.hello", "Lunar".to_string()).await
        );
        assert_eq!(
            Err(crate::err::METHOD_NOT_FOUND),
            client0.unicast::<_, String>(0, "Hello.wtf", "Lunar".to_string()).await
        );
    }

}
