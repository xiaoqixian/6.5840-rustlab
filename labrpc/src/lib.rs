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
type Tx<T> = tk_mpsc::Sender<T>;
type UbRx<T> = tk_mpsc::UnboundedReceiver<T>;
type UbTx<T> = tk_mpsc::UnboundedSender<T>;
