// Date:   Wed Aug 28 15:02:00 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use crate::{
    network::ServiceContainer,
    err::CLASS_NOT_FOUND,
    msg::RpcReq,
    CallResult
};

/// A server is a container of services running in the network.
/// Each server is associated with a unique u32 ID.
/// On receiving a RPC request, the network center checks its 
/// target server id, then dispatch it to the corresponding server.
#[derive(Default)]
pub struct Server {
    services: ServiceContainer
}

impl Server {
    pub fn new(services: ServiceContainer) -> Self {
        Self { services }
    }

    pub async fn dispatch(&self, req: RpcReq) -> CallResult {
        let services = self.services.read().await;
        match services.get(&req.cls) {
            None => Err(CLASS_NOT_FOUND),
            Some(h) => h.call(&req.method, &req.arg[..]).await
        }
    }
}

