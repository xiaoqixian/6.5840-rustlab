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

#[cfg(not(feature = "no_test_debug"))]
use colored::Colorize;

macro_rules! debug {
    (no_label, $($args: expr), *) => {
        #[cfg(not(feature = "no_test_debug"))]
        {
            println!($($args), *);
        }
    };
    ($($args: expr),*) => {
        #[cfg(not(feature = "no_test_debug"))]
        {
            let msg = format!("[TEST]: {}", format_args!($($args),*)).yellow();
            println!("{msg}");
        }
    }
}

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
            debug!("disconnect {victim}");
            tester.must_submit_cmd(&randu32(), N-1, true).await?;
        }
        if crash {
            tester.crash_one(victim).await?;
            debug!("crash {victim}");
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
            debug!("reconnect {victim}");
            tester.must_submit_cmd(&randu32(), N, true).await?;
            leader1 = tester.check_one_leader().await?;
        }

        if crash {
            tester.start_one(victim, true, true).await?;
            tester.connect(victim).await;
            debug!("restart and connect {victim}");
            tester.must_submit_cmd(&randu32(), N, true).await?;
            leader1 = tester.check_one_leader().await?;
        }
    }
    tester.end().await
}

#[tokio::test]
async fn test3d_snapshot_basic() {
    let t = "Test 3D: snapshot basic";
    timeout_test(snap_common(t, false, true, false)).await;
}

#[tokio::test]
async fn test3d_snapshot_install() {
    let t = "Test 3D: install snapshot(disconnect)";
    timeout_test(snap_common(t, true, true, false)).await;
}

#[tokio::test]
async fn test3d_snapshot_install_unreliable() {
    let t = "Test 3D: install snapshot(disconnect + unreliable)";
    timeout_test(snap_common(t, true, false, false)).await;
}

#[tokio::test]
async fn test3d_snapshot_install_crash() {
    let t = "Test 3D: install snapshot(crash)";
    timeout_test(snap_common(t, false, true, true)).await;
}

#[tokio::test]
async fn test3d_snapshot_install_unreliable_crash() {
    let t = "Test 3D: install snapshot(unreliable + crash)";
    timeout_test(snap_common(t, false, false, true)).await;
}

// do the servers persist the snapshots, and
// restart using snapshot along with the
// tail of the log?
#[tokio::test]
async fn test3d_snapshot_all_crash() {
    async fn snapshot_all_crash() -> Result<()> {
        const N: usize = 3;
        let mut tester = Tester::<u32>::new(N, true, true).await?;
        tester.begin("Test 3D: crash and restart all servers").await;

        tester.must_submit_cmd(&randu32(), N, true).await?;

        for _ in 0..5 {
            let nn = SNAPSHOT_INTERVAL / 2 + randusize() % SNAPSHOT_INTERVAL;
            for _ in 0..nn {
                tester.must_submit_cmd(&randu32(), N, true).await?;
            }

            let index1 = tester.must_submit_cmd(&randu32(), N, true).await?;
            
            // restart all
            for id in 0..N {
                tester.start_one(id, true, true).await?;
            }

            let index2 = tester.must_submit_cmd(&randu32(), N, true).await?;
            if index2 < index1 + 1 {
                fatal!("index decreased from {index1} to {index2}");
            }
        }
        tester.end().await
    }
    timeout_test(snapshot_all_crash()).await;
}

/// Do servers correctly initialize their in-memory copy of the snapshot,
/// making sure that future writes to presistent state don't lose state?
#[tokio::test]
async fn test3d_snapshot_init() {
    async fn snapshot_init() -> Result<()> {
        const N: usize = 3;
        let mut tester = Tester::<u32>::new(N, true, true).await?;
        tester.begin("Test 3D: snapshot initialization after crash").await;

        tester.must_submit_cmd(&randu32(), N, true).await?;

        // submit enough commands to create a snapshot
        for _ in 0..=SNAPSHOT_INTERVAL {
            tester.must_submit_cmd(&randu32(), N, true).await?;
        }

        // restart all
        for id in 0..N {
            tester.start_one(id, true, true).await?;
        }

        // get something to be written back to the persistent storage
        tester.must_submit_cmd(&randu32(), N, true).await?;

        for id in 0..N {
            tester.start_one(id, true, true).await?;
        }

        // submit another one to trigger potential bug
        tester.must_submit_cmd(&randu32(), N, true).await?;
        tester.end().await
    }
    timeout_test(snapshot_init()).await;
}
