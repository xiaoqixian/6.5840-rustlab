// Date:   Thu Aug 29 11:06:21 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

pub mod raft;
pub mod msg;
pub(crate) mod persist;

type Tx<T> = tokio::sync::mpsc::UnboundedSender<T>;
type Rx<T> = tokio::sync::mpsc::UnboundedReceiver<T>;

#[cfg(test)]
pub mod tests;
