// Date:   Tue Oct 01 14:56:40 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

mod peer;

pub use peer::Peer;

#[macro_export]
macro_rules! fatal {
    ($($args: expr),*) => {{
        use colored::Colorize;
        let msg = format!($($args),*).red();
        panic!("{msg}")
    }}
}
