// Date:   Thu Aug 29 11:18:29 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

pub struct Command {
    index: usize,
    command: Vec<u8>
}

pub struct Snapshot {
    term: usize,
    index: usize,
    snapshot: Vec<u8>
}

pub enum ApplyMsg {
    Command(Command),
    Snapshot(Snapshot)
}
