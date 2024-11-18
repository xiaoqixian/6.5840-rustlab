// Date:   Mon Sep 23 21:33:19 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::Arc, time::Duration};

#[cfg(not(feature = "no_test_debug"))]
use colored::Colorize;
use tokio::sync::Mutex;

use super::{Tester, ELECTION_TIMEOUT, timeout_test, Result};
use crate::fatal;

macro_rules! debug {
    ($($args: expr),*) => {
        #[cfg(not(feature = "no_test_debug"))]
        {
            let msg = format!("[TEST]: {}", format_args!($($args),*)).truecolor(240, 191, 79);
            println!("{msg}");
        }
    }
}

// macro_rules! fatal {
//     ($($args: expr), *) => {
//         return Err(format!($($args),*))
//     }
// }

#[tokio::test]
async fn test3b_basic_agree() {
    async fn basic_agree() -> Result<()> {
        const N: usize = 3;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;

        tester.begin("Test 3B: basic agreement").await;

        let base = tester.must_submit_cmd(&1, N, false).await?;

        for index in (base+1)..=(base+3) {
            let (nc, _) = tester.n_committed(index).await?;
            if nc > 0 {
                fatal!("Some have committed before start");
            }

            let xindex = tester.must_submit_cmd(&(index as u32 * 100), N, false).await?;
            if xindex != index {
                fatal!("Expected {index}, got {xindex}");
            }
        }

        tester.end().await?;
        Ok(())
    }
    timeout_test(basic_agree()).await;
}

#[tokio::test]
async fn test3b_rpc_byte() {
    async fn rpc_byte() -> Result<()> {
        const N: usize = 3;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<String>::new(N, RELIABLE, SNAPSHOT).await?;

        tester.begin("Test 3B: Rpc byte count").await;

        let start_index = tester.must_submit_cmd(&"0".to_string(), N, false).await?;
        let byte0 = tester.byte_cnt().await;
        let mut sent = 0u64;
        use rand::{Rng, thread_rng, distributions::Alphanumeric};

        for index in (start_index+1)..=(start_index+10) {
            let cmd: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(5000)
                .map(char::from)
                .collect();
            // sent += cmd.len() as u64;
            sent += 5008;
            let xindex = tester.must_submit_cmd(&cmd, N, false).await?;
            if xindex != index {
                fatal!("Expected {index}, got {xindex}");
            }
        }

        let byte1 = tester.byte_cnt().await;
        let bytes_got = byte1 - byte0;
        let byte_expected = sent * (N-1) as u64;
        if bytes_got - byte_expected > 5000 {
            fatal!("Too many RPC bytes: got {bytes_got},\
                    expected no more than {}", byte_expected + 5000);
        }

        tester.end().await?;
        Ok(())
    }
    timeout_test(rpc_byte()).await;
}

#[tokio::test]
async fn test3b_follower_failure() {
    async fn follower_failure() -> Result<()> {
        const N: usize = 3;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;
        tester.begin("Test 3B: test progressive failure of followers").await;

        macro_rules! must_continue {
            ($prev: expr, $curr: expr) => {
                if $curr != $prev + 1 {
                    fatal!("Inconsecutive command index,\
                            expect {}, got {}", $prev + 1, $curr);
                }
            }
        }
        macro_rules! mdebug {
            ($($args: expr), *) => {
                debug!("[test3b_follower_failure]: {}", format_args!($($args),*));
            }
        }
        let cmd_idx0 = tester.must_submit_cmd(&101, N, false).await?;
        mdebug!("cmd_idx0 = {cmd_idx0}");

        // disconnect a follower from the network
        let leader1 = tester.check_one_leader().await?;
        mdebug!("checked leader1 = {leader1}");
        tester.disconnect((leader1 + 1) % N).await;
        mdebug!("disconnected follower {}", (leader1+1)%N);

        // the leader and the remaining followers should be able to agree 
        // despite a follower is disconnected.
        let cmd_idx1 = tester.must_submit_cmd(&102, N-1, false).await?;
        must_continue!(cmd_idx0, cmd_idx1);

        tokio::time::sleep(ELECTION_TIMEOUT).await;
        let cmd_idx2 = tester.must_submit_cmd(&103, N-1, false).await?;
        must_continue!(cmd_idx1, cmd_idx2);

        // disconnect the remaining followers
        let leader2 = tester.check_one_leader().await?;
        mdebug!("checked leader2 = {leader2}");
        tester.disconnect((leader2 + 1) % N).await;
        tester.disconnect((leader2 + 2) % N).await;
        mdebug!("disconnected follower {} and {}", 
            (leader2+1)%N, (leader2+2)%N);

        let cmd_idx3 = match tester.let_it_start(leader2, &104).await {
            None => fatal!("Ask leader2 {leader2} to submit a command failed"),
            Some((i, _)) => i
        };
        must_continue!(cmd_idx2, cmd_idx3);

        tokio::time::sleep(ELECTION_TIMEOUT * 2).await;

        // cmd_idx3 should not be committed
        let (n, _) = tester.n_committed(cmd_idx3).await?;
        if n > 0 {
            fatal!("Command {cmd_idx3} is committed while majority of nodes are disconnected");
        } else {
            Ok(())
        }
    }
    timeout_test(follower_failure()).await;
}

#[tokio::test]
async fn test3b_leader_failure() {
    async fn leader_failure() -> Result<()> {
        const N: usize = 3;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;
        tester.begin("Test 3B: test failure of leaders").await;

        let start_index = tester.must_submit_cmd(&101, N, false).await?;

        // disconnect the first leader
        let leader1 = tester.check_one_leader().await?;
        tester.disconnect(leader1).await;
        debug!("disconnected leader1 {leader1}");

        // the remaining followers should elect a new leader.
        let _ = tester.submit_cmd(&102, N-1, false).await?;
        tokio::time::sleep(ELECTION_TIMEOUT).await;
        let _ = tester.submit_cmd(&103, N-1, false).await?;

        let leader2 = tester.check_one_leader().await?;
        tester.disconnect(leader2).await;

        // submit a command to each server
        let cmd4 = bincode::serialize(&104).unwrap();
        for core in tester.nodes.iter()
            .filter_map(|node| node.core.as_ref())
        {
            let _ = core.raft.start(cmd4.clone()).await;
        }

        tokio::time::sleep(ELECTION_TIMEOUT * 2).await;

        let (n, _) = tester.n_committed(start_index + 3).await?;
        if n > 0 {
            fatal!("Command 104 committed count = {n}, expect 0, cause majority of nodes are disconnected");
        }

        tester.end().await?;
        Ok(())
    }
    timeout_test(leader_failure()).await;
}

#[tokio::test]
async fn test3b_fail_agree() {
    async fn fail_agree() -> Result<()> {
        const N: usize = 3;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;

        tester.begin("Test 3B: agreement after follower reconnects").await;

        let _ = tester.submit_cmd(&101, N, false).await?;

        // disconnect a follower from the network
        let leader = tester.check_one_leader().await?;
        tester.disconnect((leader + 1) % N).await;

        let _ = tester.submit_cmd(&102, N-1, false).await?;
        let _ = tester.submit_cmd(&103, N-1, false).await?;
        tokio::time::sleep(ELECTION_TIMEOUT).await;
        let _ = tester.submit_cmd(&104, N-1, false).await?;
        let _ = tester.submit_cmd(&105, N-1, false).await?;

        tester.connect((leader + 1) % N).await;

        // the full set of servers should preserve previous agreement, 
        // and be able to agree on new commands.
        let _ = tester.submit_cmd(&106, N, true).await?;
        tokio::time::sleep(ELECTION_TIMEOUT).await;
        let _ = tester.submit_cmd(&107, N, true).await?;

        tester.end().await?;
        Ok(())
    }
    timeout_test(fail_agree()).await;
}

/// Let most of the nodes in the cluster crash, expect no commands 
/// should be committed in such a circumstance.
#[tokio::test]
async fn test3b_fail_no_agree() {
    async fn fail_no_agree() -> Result<()> {
        const N: usize = 5;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;
        tester.begin("Test 3B: no agreement if too many followers disconnect").await;

        let starti = tester.must_submit_cmd(&10, N, false).await?;

        let leader = tester.check_one_leader().await?;
        debug!("leader = {leader}");
        // disconnect 3 of 5 servers.
        for i in 1..4 {
            tester.disconnect((leader + i) % N).await;
        }

        {
            // let the leader submit a command, the leader should refuse 
            // this request.
            let cmd_idx2 = {
                let cmd_info = tester.let_it_start(leader, &20).await;
                match cmd_info {
                    None => fatal!("Leader {leader} refused to start command 20"),
                    Some((cmd_idx2, _)) => {
                        if cmd_idx2 != starti + 1 {
                            fatal!("Inconsecutive command index, expected {}, got {cmd_idx2}", starti + 1)
                        }
                        cmd_idx2
                    }
                }
            };

            tokio::time::sleep(ELECTION_TIMEOUT * 2).await;

            // cmd_idx2 should not be committed, as most of the servers 
            // are absent.
            let (n, _) = tester.n_committed(cmd_idx2).await?;
            if n > 0 {
                fatal!("Command 20 committed count = {n}, expect 0, 
                    cause majority of nodes are disconnected");
            }
        }

        // reconnect the before disconnected servers.
        for i in 1..4 {
            tester.connect((leader + i) % N).await;
        }

        {
            // the new leader may change or may not, 
            // so the new submitted command index should either be 
            // in [starti+1, starti+2].
            let leader2 = tester.check_one_leader().await?;
            debug!("leader2 = {leader2}");
            match tester.let_it_start(leader2, &30).await
            {
                None => fatal!("Leader2 {leader2} refused to start a command"),
                Some((cmd_idx, _)) => {
                    if ![starti+1, starti+2].contains(&cmd_idx) {
                        fatal!("Unexpected command index {cmd_idx}, 
                            should be either {} or {}", starti+1, starti+2);
                    }
                }
            }
        }

        let _ = tester.submit_cmd(&1000, N, true).await?;

        tester.end().await?;
        Ok(())
    }
    timeout_test(fail_no_agree()).await;
}

/// Ask a leader to concurrently start multiple commands.
/// During this period, as the network env is not changed, the 
/// term should not be changed by any node, and all the commands 
/// should be committed by all nodes.
/// This operation will be tried for multiple times before the tester panics.
#[tokio::test]
async fn test3b_concurrent_starts() {
    async fn concurrent_starts() -> Result<()> {
        const N: usize = 3;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;
        tester.begin("Test 3B: concurrent start()").await;

        const TRIES: usize = 5;
        // number of concurrent starts
        const M: u32 = 5;
        let mut ticker = tokio::time::interval(Duration::from_secs(3));
        let mut success = false;
        'looop: for _ in 0..TRIES {
            ticker.tick().await;

            let leader = tester.check_one_leader().await?;
            let term = match tester.let_it_start(leader, &1).await
            {
                None => continue 'looop,
                Some((_, term)) => term
            };

            let res_set = {
                let safe_tester = Arc::new(Mutex::new(Some(tester)));
                let mut join_set = tokio::task::JoinSet::new();
                for i in 0..M {
                    let tester = safe_tester.clone();
                    join_set.spawn(async move {
                        match tester.lock().await.as_mut().unwrap()
                            .let_it_start(leader, &(100 + i)).await
                        {
                            Some((idx, tm)) if tm == term => Some(idx),
                            _ => None
                        }
                    });
                }

                let res_set = join_set.join_all().await;
                tester = safe_tester.lock().await.take().unwrap();
                res_set
            };

            for i in 0..N {
                let (term_i, _) = tester.nodes[i].core.as_ref().unwrap()
                    .raft.get_state().await;
                if term_i != term {
                    continue 'looop;
                }
            }

            let mut cmds = Vec::new();
            for cmd_index in res_set.into_iter().filter_map(|idx| idx) {
                match tester.wait(cmd_index, N, Some(term)).await? {
                    None => continue 'looop,
                    Some(cmd) => cmds.push(cmd)
                }
            }

            for cmd in 100..(100+M) {
                if !cmds.contains(&cmd) {
                    fatal!("Command {cmd} missing");
                }

            }
            success = true;
            break 'looop;
        }

        if !success {
            fatal!("Term changed too often");
        }
        tester.end().await?;
        Ok(())
    }
    timeout_test(concurrent_starts()).await;
}

#[tokio::test]
async fn test3b_rejoin() {
    async fn rejoin() -> Result<()> {
        const N: usize = 3;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;

        tester.begin("Test 3B: rejoin of partitioned leader").await;

        macro_rules! disconnect_leader {
            () => {{
                let leader = tester.check_one_leader().await?;
                tester.disconnect(leader).await;
                leader
            }}
        }

        tester.must_submit_cmd(&101, N, true).await?;

        let leader1 = disconnect_leader!();
        debug!("disconnected leader1 {leader1}");

        tester.let_it_start(leader1, &102).await;
        tester.let_it_start(leader1, &103).await;
        tester.let_it_start(leader1, &104).await;
        debug!("ask leader1 started 3 commands");

        let _idx = tester.submit_cmd(&103, N-1, true).await?;
        debug!("let the rest group submit a command: {_idx:?}");

        let leader2 = disconnect_leader!();

        tester.connect(leader1).await;
        debug!("reconnected leader1");
        let _idx = tester.submit_cmd(&104, N-1, true).await?;
        debug!("submit_cmd {_idx:?}");

        tester.connect(leader2).await;
        debug!("reconnected leader2");
        let _idx = tester.submit_cmd(&105, N, true).await?;
        debug!("submit_cmd {_idx:?}");

        tester.end().await?;
        Ok(())
    }
    timeout_test(rejoin()).await;
}

#[tokio::test]
async fn test3b_backup() {
    async fn backup() -> Result<()> {
        const N: usize = 5;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;
        tester.begin("Test 3B: leader backs up quickly over incorrect follower logs").await;

        let randu32 = rand::random::<u32>;

        tester.must_submit_cmd(&randu32(), N, true).await?;

        let leader1 = tester.check_one_leader().await?;
        debug!("leader1 = {leader1}");
        for i in 2..5 {
            tester.disconnect((leader1 + i) % N).await;
            debug!("disconnected {}", (leader1 + i) % N);
        }

        // submit lots of commands that won't commit
        for _ in 0..50 {
            tester.let_it_start(leader1, &randu32()).await;
        }
        debug!("leader1 {leader1} submitted 50 commands!");

        tokio::time::sleep(ELECTION_TIMEOUT / 2).await;

        tester.disconnect(leader1).await;
        tester.disconnect((leader1 + 1) % N).await;
        debug!("disconnected leader1 {leader1} and {}", (leader1+1)%N);

        for i in 2..5 {
            tester.connect((leader1 + i) % N).await;
            debug!("reconnect {}", (leader1 + i) % N);
        }

        // submit lots of commands to the new group
        for _ in 0..50 {
            tester.submit_cmd(&randu32(), N-2, true).await?;
        }
        debug!("reconnected group submitted 50 commands!");

        let leader2 = tester.check_one_leader().await?;
        debug!("leader2 = {leader2}");
        let other = {
            let mut other = (leader1 + 2) % N;
            if other == leader2 {
                other = (leader2 + 1) % N;
            }
            other
        };
        tester.disconnect(other).await;
        debug!("disconnected other {other}");

        for _ in 0..50 {
            tester.let_it_start(leader2, &randu32()).await;
        }
        debug!("leader2 {leader2} submitted 50 commands");

        tokio::time::sleep(ELECTION_TIMEOUT / 2).await;

        for i in 0..N {
            tester.disconnect(i).await;
        }
        debug!("disconnected all");
        tester.connect(leader1).await;
        tester.connect((leader1 + 1) % N).await;
        tester.connect(other).await;
        debug!("reconnect {}, {} and {}", 
            leader1, (leader1+1)%N, other);

        for _ in 0..50 {
            tester.submit_cmd(&randu32(), N-2, true).await?;
        }
        debug!("new group submitted 50 commands!");

        for i in 0..N {
            tester.connect(i).await;
        }
        debug!("reconnect all");
        tester.submit_cmd(&randu32(), N, true).await?;

        tester.end().await?;
        Ok(())
    }
    timeout_test(backup()).await;
}

/// Let the leader commit M number of commands, 
/// then wait the followers to apply them, check if they are identical.
/// Then check how many RPCs are sent.
#[tokio::test]
async fn test3b_count() {
    async fn count() -> Result<()> {
        const N: usize = 3;
        const RELIABLE: bool = true;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;

        tester.begin("Test 3B: RPC counts aren't too high").await;

        let _ = tester.check_one_leader().await?;

        let mut success = false;
        let mut ticker = tokio::time::interval(Duration::from_secs(3));
        let mut total1 = 0u32;
        'looop: for _ii in 0..5 {
            ticker.tick().await;
            debug!("looop iterate {_ii}");

            let leader = tester.check_one_leader().await?;
            debug!("leader = {leader}");
            let total0 = tester.rpc_cnt().await;

            // starti is the expected index of the next submitted command.
            let (starti, term) = match tester.let_it_start(leader, &1).await {
                None => continue 'looop,
                Some((idx, term)) => (idx+1, term)
            };

            // let the leader commit M number of commands, 
            // collect them in a Vec.
            const M: usize = 10;
            // cmds[0] is not checked.
            let mut cmds = Vec::new();
            // idx is the expected submitted command index.
            for idx in starti..(starti+M) {
                let x = rand::random::<u32>();
                cmds.push(x);

                match tester.let_it_start(leader, &x).await {
                    Some((x_idx, term_i)) if term == term_i => {
                        if x_idx != idx {
                            fatal!("Inconsecutive submitted command index, 
                                expect {idx}, got {x_idx}");
                        }
                    },
                    _ => continue 'looop
                }
            }
            debug!("Leader {leader} started all {M} commands!");

            for i in 0..M {
                debug!("Check command {}", starti + i);
                match tester.wait(starti + i, N, Some(term)).await? {
                    None => continue 'looop,
                    Some(cmd) => {
                        if cmd != cmds[i] {
                            fatal!("Wrong value committed for index {}, expected {}, got {cmd}", starti + i, cmds[i]);
                        }
                    }
                }
            }

            for core in tester.nodes.iter()
                .filter_map(|node| node.core.as_ref()) {
                    let (tm, _) = core.raft.get_state().await;
                    if tm != term {
                        debug!("Found a node with different term: {tm} != {term}");
                        continue 'looop;
                    }
                }

            total1 = tester.rpc_cnt().await;
            if (total1 - total0) as usize > (M+4) * 3 {
                fatal!("Too many RPCs ({}) for {M} entries", total1 - total0);
            }

            success = true;
            break 'looop;
        }

        if !success {
            fatal!("Term changed too often");
        }

        // sleep 1 sec to see how many RPC increased in one second of idleness
        tokio::time::sleep(Duration::from_secs(1)).await;
        let total2 = tester.rpc_cnt().await;

        if total2 - total1 > 60 {
            fatal!("Too many RPCs ({}) for 1 second of idleness, expected no more than 60.", total2 - total1);
        }

        tester.end().await?;
        Ok(())
    }
    timeout_test(count()).await;
}
