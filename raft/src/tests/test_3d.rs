// Date:   Fri Nov 08 22:23:27 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::fmt::Display;

use crate::tests::{
    utils::{randu32, randusize},
    Result, Tester, SNAPSHOT_INTERVAL,
};
use crate::fatal;
use super::timeout_test;

/// Test 3D: snapshot.

const MAX_LOG_SIZE: usize = 2000;

// macro_rules! fatal {
//     ($($args: expr), *) => {
//         return Err(PanicErr {
//             msg: format!($($args),*),
//             file: file!(),
//             line: line!()
//         })
//     }
// }

async fn snap_common<T>(name: T, disconnect: bool, reliable: bool, crash: bool) -> Result<()>
where T: Display
{
    const N: usize = 3;
    let mut tester = Tester::<u32>::new(N, reliable, true).await?;
    tester.begin(name).await;

    let _ = tester.must_submit_cmd(&randu32(), N, true).await?;
    let mut leader1 = tester.check_one_leader().await?;

    for i in 0..30 {
        let (victim, sender) = if i % 3 == 1 {
            ((leader1 + 1) % N, leader1)
        } else {
            (leader1, (leader1 + 1) % N)
        };

        if disconnect {
            tester.disconnect(victim).await;
            tester.must_submit_cmd(&randu32(), N-1, true).await?;
        }
        if crash {
            tester.crash_one(victim).await?;
            tester.must_submit_cmd(&randu32(), N-1, true).await?;
        }

        // perhaps send enough to get a snapshot
        let nn = SNAPSHOT_INTERVAL / 2 + randusize() % SNAPSHOT_INTERVAL;
        for _ in 0..nn {
            tester.let_it_start(sender, &randu32()).await;
        }

        // let applier threads catch up with the Start()'s
        let expect = if !disconnect && !crash {
            // make sure all followers have caught up
            N
        } else {
            N - 1
        };
        tester.must_submit_cmd(&randu32(), expect, true).await?;

        let max_raft_state_size = tester.nodes.iter()
            .map(|node| node.persister.raft_state_size())
            .max()
            .unwrap();
        
        if max_raft_state_size >= MAX_LOG_SIZE {
            fatal!("Log size too large, expect <= {MAX_LOG_SIZE} bytes");
        }

        if disconnect {
            tester.connect(victim).await;
            tester.must_submit_cmd(&randu32(), N, true).await?;
            leader1 = tester.check_one_leader().await?;
        }

        if crash {
            tester.start_one(victim, true, true).await?;
            tester.connect(victim).await;
            tester.must_submit_cmd(&randu32(), N, true).await?;
            leader1 = tester.check_one_leader().await?;
        }
    }
    tester.end().await
}

#[tokio::test]
async fn test3d_snapshot_basic() {
    timeout_test(snap_common("Test 3D: snapshot basic", false, false, false)).await;
}
