// Date:   Thu Aug 29 11:18:29 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

pub struct Command {
    pub index: usize,
    pub command: Vec<u8>
}

pub struct Snapshot {
    pub term: usize,
    pub index: usize,
    pub snapshot: Vec<u8>
}

pub enum ApplyMsg {
    Command(Command),
    Snapshot(Snapshot)
}
