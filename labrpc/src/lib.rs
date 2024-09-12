// Date:   Thu Aug 15 13:18:58 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod network;
pub mod err;
pub mod client;
mod server;

use tokio::sync::mpsc as tk_mpsc;

type UbRx<T> = tk_mpsc::UnboundedReceiver<T>;
type UbTx<T> = tk_mpsc::UnboundedSender<T>;
type OneTx<T> = tokio::sync::oneshot::Sender<T>;

// Idx represents the index of a raft node.
type Idx = usize;
// Key represents an unique ID of a client or server,
// I use type alias to differ the struct fields.
type Key = usize;

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
    pub key: Key,
    pub from: Idx,
    pub to: Idx,
    pub req: RpcReq,
    pub reply_tx: OneTx<CallResult>
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use labrpc_macros::rpc;
    use crate::{err::{self, CLASS_NOT_FOUND}, network::Network, CallResult, Service};

    struct Hello;
    #[rpc(Service, CallResult, err)]
    impl Hello {
        pub fn hello(&self, name: String) -> String {
            format!("Hello, {name}")
        }
    }

    #[tokio::test]
    async fn basic() {
        const N: usize = 3;
        let mut network = Network::new(N);
        let mut clients = Vec::with_capacity(N);
        for i in 0..N {
            let cli = network.make_client(i).await;
            cli.add_service(
                "Hello".to_string(),
                Box::new(Hello)
            ).await;
            clients.push(cli);
        }

        {
            let c0 = &clients[0];
            let mut cnt = 0;
            for peer in c0.peers() {
                assert_eq!(
                    Ok("Hello, Lunar".to_string()),
                    peer.call::<_, String>("Hello.hello", &"Lunar").await
                );
                cnt += 1;
            }
            assert_eq!(2, cnt);
        }

        // make a new client at index 0, add a same service with 
        // different name, expect the original server is removed.
        {
            let c0 = &mut clients[0];
            *c0 = network.make_client(0).await;
            c0.add_service("Hello2".to_string(), 
                Box::new(Hello)).await;
        }
        
        {
            let c1 = &clients[1];
            let mut res = Vec::new();
            for peer in c1.peers() {
                res.push(
                    peer.call::<_, String>("Hello.hello", &"Lunar").await
                );
            }
            assert_eq!(2, res.len());

            let count = |expect| -> usize {
                res.iter()
                    .fold(0, |acc, r| {
                        acc + if r == expect {
                            1
                        } else { 0 }
                    })
            };
            let ok = Ok("Hello, Lunar".to_string());
            assert_eq!(1, count(&ok));
            assert_eq!(1, count(&Err(CLASS_NOT_FOUND)));
        }
    }

    #[tokio::test]
    async fn unreliable() {
        const N: usize = 2;
        let mut network = Network::new(N)
            .reliable(false)
            .long_delay(true);
        let mut clients = Vec::with_capacity(N);
        for i in 0..N {
            let cli = network.make_client(i).await;
            cli.add_service(
                "Hello".to_string(),
                Box::new(Hello)
            ).await;
            clients.push(cli);
        }
        
        // client0 request for 1000 times, 
        // check how many responses in a unreliable environment.
        const TRIES: usize = 1000;
        let mut cnt = 0usize;
        let mut rtt = 0u128;
        let c0 = &clients[0];
        let p1 = c0.peers().next().unwrap();

        for _ in 0..TRIES {
            let start = Instant::now();
            let ret = p1.call::<_, String>("Hello.hello", &"Lunar".to_string())
                .await;
            rtt += start.elapsed().as_millis();
            if ret.is_ok() { cnt += 1; }
        }

        println!("{} responses in {} tries", cnt, TRIES);
        println!("Average RTT: {}ms", rtt/(TRIES as u128));
    }
}
