// Date:   Thu Aug 15 13:18:58 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian


pub mod network;
pub mod msg;
pub mod end;
pub mod service;
pub mod err;

use tokio::sync::mpsc as tk_mpsc;

type Rx = tk_mpsc::UnboundedReceiver<msg::Msg>;
type Tx = tk_mpsc::UnboundedSender<msg::Msg>;
