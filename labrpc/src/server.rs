// Date:   Wed Aug 28 15:02:00 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::{
    err::CLASS_NOT_FOUND, 
    CallResult, RpcReq, Service
};

/// A server is a container of services running in the network.
/// Each server is associated with a unique u32 ID.
/// On receiving a RPC request, the network center checks its 
/// target server id, then dispatch it to the corresponding server.
#[derive(Clone, Default)]
pub(crate) struct Server {
    services: Arc<RwLock<HashMap<String, Box<dyn Service>>>>
}

impl Server {
    pub async fn dispatch(&self, req: RpcReq) -> CallResult {
        let services = self.services.read().await;
        match services.get(&req.cls) {
            None => Err(CLASS_NOT_FOUND),
            Some(h) => h.call(&req.method, &req.arg[..]).await
        }
    }

    pub async fn add_service(&self, name: String, service: Box<dyn Service>) {
        self.services.write().await.insert(name, service);
    }
}

