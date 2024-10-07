// Date:   Mon Sep 23 21:33:19 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{ops::Deref, time::Duration};
use colored::Colorize;
use tokio::sync::RwLock;

use super::{Config, Tester, WantedCmd, ELECTION_TIMEOUT};

const TIME_LIMIT: Duration = Duration::from_secs(120);

macro_rules! debug {
    ($($args: expr),*) => {
        #[cfg(not(feature = "no_test_debug"))]
        {
            let msg = format!($($args),*);
            let msg = format!("[TEST]: {msg}").truecolor(240, 191, 79);
            println!("{msg}");
        }
    }
}

macro_rules! fatal {
    ($($args: expr),*) => {{
        let msg = format!($($args),*).red();
        panic!("{msg}");
    }}
}

/// Directly ask a leader to submit a command, return the potential 
/// index and term.
/// If the "leader" is no longer a leader, it returns a None.
async fn start_by<T>(config: &RwLock<Config<T>>, cmd: &T, leader: usize) -> Option<(usize, usize)> 
    where T: WantedCmd
{
    start_by_nolock(config.read().await.deref(), cmd, leader).await
}
async fn start_by_nolock<T>(config: &Config<T>, cmd: &T, leader: usize) -> Option<(usize, usize)> 
    where T: WantedCmd
{
    let cmd = bincode::serialize(cmd).unwrap();
    config.nodes.get(leader).unwrap()
        .raft.as_ref().unwrap().start(cmd).await
}

#[tokio::test]
async fn test3b_basic_agree() {
    const N: usize = 3;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;

    tester.begin("Test 3B: basic agreement").await;

    let base = tester.submit_cmd(1, N, false).await
        .expect("Try to submit a command failed");

    for index in (base+1)..=(base+3) {
        let (nc, _) = tester.n_committed(index).await;
        if nc > 0 {
            fatal!("Some have committed before start");
        }

        let xindex = tester.submit_cmd(index as u32 * 100, N, false).await;
        if xindex != Some(base + index) {
            fatal!("Expected Some({index}), got {xindex:?}");
        }
    }

    tester.end().await;
}

#[tokio::test]
async fn test3b_rpc_byte() {
    const N: usize = 3;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<String>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;

    tester.begin("Test 3B: Rpc byte count").await;

    let start_index = match tester.submit_cmd("0".to_string(), N, false).await {
        Some(idx) => idx,
        None => fatal!("Try to commit a command failed")
    };
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
        let xindex = tester.submit_cmd(cmd, N, false).await;
        if xindex != Some(index) {
            fatal!("Expected {index}, got {xindex:?}");
        }
    }

    let byte1 = tester.byte_cnt().await;
    let bytes_got = byte1 - byte0;
    let byte_expected = sent * (N-1) as u64;
    if bytes_got - byte_expected > 5000 {
        fatal!("Too many RPC bytes: got {bytes_got}, expected no more than {}", byte_expected + 5000);
    }

    tester.end().await;
}

#[tokio::test]
async fn test3b_follower_failure() {
    const N: usize = 3;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;
    const desc: &'static str = "Test 3B: test progressive failure of followers";
    tester.begin(desc).await;

    macro_rules! must_commit {
        ($cmd: expr, $n: expr, $retry: expr) => {
            tester.submit_cmd($cmd, $n, $retry).await
                .expect(&format!("Commit command {} failed", $cmd).red())
        }
    }
    macro_rules! must_continue {
        ($prev: expr, $curr: expr) => {
            assert_eq!($prev + 1, $curr, 
                "Inconsecutive command index, expect {}, got {}",
                $prev + 1, $curr);
        }
    }
    macro_rules! mdebug {
        ($($args: expr), *) => {
            debug!("[{}]: {}", desc, format_args!($($args),*));
        }
    }
    let cmd_idx0 = must_commit!(101, N, false);
    mdebug!("cmd_idx0 = {cmd_idx0}");

    // disconnect a follower from the network
    let leader1 = tester.check_one_leader().await;
    mdebug!("checked leader1 = {leader1}");
    tester.disconnect((leader1 + 1) % N).await;
    mdebug!("disconnected follower {}", (leader1+1)%N);

    // the leader and the remaining followers should be able to agree 
    // despite a follower is disconnected.
    let cmd_idx1 = must_commit!(102, N-1, false);
    must_continue!(cmd_idx0, cmd_idx1);

    tokio::time::sleep(ELECTION_TIMEOUT).await;
    let cmd_idx2 = must_commit!(103, N-1, false);
    must_continue!(cmd_idx1, cmd_idx2);

    // disconnect the remaining followers
    let leader2 = tester.check_one_leader().await;
    mdebug!("checked leader2 = {leader2}");
    tester.disconnect((leader2 + 1) % N).await;
    tester.disconnect((leader2 + 2) % N).await;
    mdebug!("disconnected follower {} and {}", 
        (leader2+1)%N, (leader2+2)%N);

    let cmd_idx3 = match start_by(tester.config.as_ref(), &104, leader2).await {
        None => fatal!("Ask leader2 {leader2} to submit a command failed"),
        Some((i, _)) => i
    };
    must_continue!(cmd_idx2, cmd_idx3);

    tokio::time::sleep(ELECTION_TIMEOUT * 2).await;

    // cmd_idx3 should not be committed
    let (n, _) = tester.n_committed(cmd_idx3).await;
    if n > 0 {
        fatal!("Command {cmd_idx3} is committed while majority of nodes are disconnected");
    }
}

#[tokio::test]
async fn test3b_leader_failure() {
    const N: usize = 3;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;
    tester.begin("Test 3B: test failure of leaders").await;

    let start_index = tester.submit_cmd(101, N, false).await
        .expect(&format!("Submit command {} failed", 101).red());

    // disconnect the first leader
    let leader1 = tester.check_one_leader().await;
    tester.disconnect(leader1).await;
    debug!("disconnected leader1 {leader1}");
    
    // the remaining followers should elect a new leader.
    let _ = tester.submit_cmd(102, N-1, false).await;
    tokio::time::sleep(ELECTION_TIMEOUT).await;
    let _ = tester.submit_cmd(103, N-1, false).await;

    let leader2 = tester.check_one_leader().await;
    tester.disconnect(leader2).await;

    // submit a command to each server
    {
        let config = tester.config.read().await;
        let cmd4 = bincode::serialize(&104).unwrap();
        for raft in config.nodes.iter().
            filter_map(|node| node.raft.as_ref())
        {
            let _ = raft.start(cmd4.clone()).await;
        }
    }

    tokio::time::sleep(ELECTION_TIMEOUT * 2).await;

    let (n, _) = tester.n_committed(start_index + 3).await;
    if n > 0 {
        fatal!("Command 104 committed count = {n}, expect 0, cause majority of nodes are disconnected");
    }

    tester.end().await;
}

#[tokio::test]
async fn test3b_fail_agree() {
    const N: usize = 3;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;

    tester.begin("Test 3B: agreement after follower reconnects").await;

    let _ = tester.submit_cmd(101, N, false).await;

    // disconnect a follower from the network
    let leader = tester.check_one_leader().await;
    tester.disconnect((leader + 1) % N).await;

    let _ = tester.submit_cmd(102, N-1, false).await;
    let _ = tester.submit_cmd(103, N-1, false).await;
    tokio::time::sleep(ELECTION_TIMEOUT).await;
    let _ = tester.submit_cmd(104, N-1, false).await;
    let _ = tester.submit_cmd(105, N-1, false).await;
    
    tester.connect((leader + 1) % N).await;

    // the full set of servers should preserve previous agreement, 
    // and be able to agree on new commands.
    let _ = tester.submit_cmd(106, N, true).await;
    tokio::time::sleep(ELECTION_TIMEOUT).await;
    let _ = tester.submit_cmd(107, N, true).await;

    tester.end().await;
}

#[tokio::test]
async fn test3b_fail_no_agree() {
    const N: usize = 5;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;
    tester.begin("Test 3B: no agreement if too many followers disconnect").await;

    let start_index = tester.submit_cmd(10, N, false).await.unwrap();

    let leader = tester.check_one_leader().await;
    // 3 of 5 servers are disconnected
    for i in 1..4 {
        tester.disconnect((leader + i) % N).await;
    }

    {
        let cmd_idx2 = {
            let config = tester.config.read().await;
            let cmd = bincode::serialize(&20).unwrap();
            let cmd_info = config.nodes.get(leader).unwrap()
                .raft.as_ref().unwrap()
                .start(cmd).await;
            match cmd_info {
                None => fatal!("Leader {leader} refused to start command 20"),
                Some((cmd_idx, _)) => {
                    if cmd_idx != start_index + 1 {
                        fatal!("Inconsecutive command index, expected {}, got {cmd_idx}", start_index + 1)
                    }
                    cmd_idx
                }
            }
        };

        tokio::time::sleep(ELECTION_TIMEOUT * 2).await;

        let (n, _) = tester.n_committed(cmd_idx2).await;
        if n > 0 {
            fatal!("Command 20 committed count = {n}, expect 0, 
                cause majority of nodes are disconnected");
        }
    }

    for i in 1..4 {
        tester.connect((leader + i) % N).await;
    }

    {
        let leader2 = tester.check_one_leader().await;
        let config = tester.config.read().await;
        let cmd = bincode::serialize(&30).unwrap();
        match config.nodes.get(leader2).unwrap()
            .raft.as_ref().unwrap()
            .start(cmd).await
        {
            None => fatal!("Leader2 {leader2} refused to start a command"),
            Some((cmd_idx, _)) => {
                if cmd_idx != start_index + 1 {
                    fatal!("Inconsecutive command index, expect {}, got {cmd_idx}", start_index + 1);
                }
            }
        }
    }

    let _ = tester.submit_cmd(1000, N, true).await;

    tester.end().await;
}

/// Ask a leader to concurrently start multiple commands.
/// During this period, as the network env is not changed, the 
/// term should not be changed by any node, and all the commands 
/// should be committed by all nodes.
/// This operation will be tried for multiple times before the tester panics.
#[tokio::test]
async fn test3b_concurrent_starts() {
    const N: usize = 3;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;
    tester.begin("Test 3B: concurrent start()").await;

    const TRIES: usize = 5;
    // number of concurrent starts
    const M: u32 = 5;
    let mut ticker = tokio::time::interval(Duration::from_secs(3));
    let mut success = false;
    'looop: for _ in 0..TRIES {
        ticker.tick().await;

        let leader = tester.check_one_leader().await;
        let term = match start_by(tester.config.as_ref(), &1, leader).await
        {
            None => continue 'looop,
            Some((_, term)) => term
        };

        let res_set = {
            let mut join_set = tokio::task::JoinSet::new();
            for i in 0..M {
                let config = tester.config.clone();
                join_set.spawn(async move {
                    match start_by(&config, &(100 + i), leader).await
                    {
                        Some((idx, tm)) if tm == term => Some(idx),
                        _ => None
                    }
                });
            }

            join_set.join_all().await
        };

        {
            let config = tester.config.read().await;
            for j in 0..N {
                let (term_j, _) = config.nodes.get(j).unwrap()
                    .raft.as_ref().unwrap().get_state().await;
                if term_j != term {
                    continue 'looop;
                }
            }
        }

        let mut cmds = Vec::new();
        for cmd_index in res_set.into_iter().filter_map(|idx| idx) {
            match tester.wait(cmd_index, N, Some(term)).await {
                None => continue 'looop,
                Some(cmd) => cmds.push(cmd)
            }
        }

        (0..M).into_iter()
            .map(|i| 100 + i)
            .for_each(|cmd| {
                if !cmds.contains(&cmd) {
                    fatal!("Command {cmd} missing");
                }
            });
        success = true;
        break 'looop;
    }

    if !success {
        fatal!("Term changed too often");
    }
    tester.end().await;
}

#[tokio::test]
async fn test3b_rejoin() {
    const N: usize = 3;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;
    
    tester.begin("Test 3B: rejoin of partitioned leader").await;

    let disconnect_leader = || async {
        let leader = tester.check_one_leader().await;
        tester.disconnect(leader).await;
        leader
    };

    let _ = tester.submit_cmd(101, N, true).await;

    let leader1 = disconnect_leader().await;

    start_by(tester.config.as_ref(), &102, leader1).await;
    start_by(tester.config.as_ref(), &103, leader1).await;
    start_by(tester.config.as_ref(), &104, leader1).await;

    let _ = tester.submit_cmd(103, N-1, true).await;

    let leader2 = disconnect_leader().await;

    tester.connect(leader1).await;
    let _ = tester.submit_cmd(104, N-1, true).await;

    tester.connect(leader2).await;
    let _ = tester.submit_cmd(105, N, true).await;

    tester.end().await;
}

#[tokio::test]
async fn test3b_backup() {
    const N: usize = 5;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;
    tester.begin("Test 3B: leader backs up quickly over incorrect follower logs").await;

    let randu32 = || {rand::random::<u32>()};

    let _ = tester.submit_cmd(randu32(), N, true).await;

    let leader1 = tester.check_one_leader().await;
    for i in 2..5 {
        tester.disconnect((leader1 + i) % N).await;
    }

    // submit lots of commands that won't commit
    for _ in 0..50 {
        start_by(tester.config.as_ref(), &randu32(), leader1).await;
    }

    tokio::time::sleep(ELECTION_TIMEOUT / 2).await;

    tester.disconnect(leader1).await;
    tester.disconnect((leader1 + 1) % N).await;

    for i in 2..5 {
        tester.connect((leader1 + i) % N).await;
    }

    // submit lots of commands to the new group
    for _ in 0..50 {
        tester.submit_cmd(randu32(), N-2, true).await;
    }

    let leader2 = tester.check_one_leader().await;
    let other = {
        let mut other = (leader1 + 2) % N;
        if other == leader2 {
            other = (leader2 + 1) % N;
        }
        other
    };
    tester.disconnect(other).await;

    for _ in 0..50 {
        start_by(tester.config.as_ref(), &randu32(), leader2).await;
    }

    tokio::time::sleep(ELECTION_TIMEOUT / 2).await;

    for i in 0..N {
        tester.disconnect(i).await;
    }
    tester.connect(leader1).await;
    tester.connect((leader1 + 1) % N).await;
    tester.connect(other).await;

    for _ in 0..50 {
        tester.submit_cmd(randu32(), N-2, true).await;
    }

    for i in 0..N {
        tester.connect(i).await;
    }
    tester.submit_cmd(randu32(), N, true).await;

    tester.end().await;
}

#[tokio::test]
async fn test3b_count() {
    const N: usize = 3;
    const RELIABLE: bool = true;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;

    tester.begin("Test 3B: RPC counts aren't too high").await;

    let _ = tester.check_one_leader().await;

    let mut success = false;
    let mut ticker = tokio::time::interval(Duration::from_secs(3));
    let config = tester.config.as_ref();
    let mut total1 = 0u32;
    'looop: for _ in 0..5 {
        ticker.tick().await;

        let leader = tester.check_one_leader().await;
        let total0 = tester.rpc_cnt().await;

        let (starti, term) = match start_by(config, &1, leader).await {
            None => continue 'looop,
            Some(v) => v
        };

        const I: usize = 10;
        let mut cmds = Vec::new();
        for i in 1..I+2 {
            let x = rand::random::<u32>();
            cmds.push(x);

            match start_by(config, &x, leader).await {
                Some((x_idx, term_i)) if term == term_i => {
                    if x_idx != starti + i {
                        fatal!("Start() failed");
                    }
                },
                _ => continue 'looop
            }
        }

        for i in 1..=I {
            match tester.wait(starti + i, N, Some(term)).await {
                None => continue 'looop,
                Some(cmd) => {
                    if cmd != cmds[i] {
                        fatal!("Wrong value committed for index {}, expected {}, got {cmd}", starti + i, cmds[i]);
                    }
                }
            }
        }

        for raft in config.read().await.nodes.iter()
            .filter_map(|node| node.raft.as_ref()) {
            let (tm, _) = raft.get_state().await;
            if tm != term {
                continue 'looop;
            }
        }

        total1 = tester.rpc_cnt().await;
        if (total1 - total0) as usize > (I+4) * 3 {
            fatal!("Too many RPCs ({}) for {I} entries", total1 - total0);
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

    tester.end().await;
}
