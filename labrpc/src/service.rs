// Date:   Fri Aug 16 16:47:42 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::{Arc, RwLock};
use std::collections::HashMap;

pub trait Service: Send + Sync {}

#[derive(Clone)]
pub struct ServiceContainer {
    services: Arc<RwLock<HashMap<String, Box<dyn Service>>>>
}

impl ServiceContainer {
    pub fn new() -> Self {
        Self {
            services: Default::default()
        }
    }

    pub fn insert(&self, name: String, obj: Box<dyn Service>) {
        self.services.write()
            .unwrap()
            .insert(name, obj);
    }
}
