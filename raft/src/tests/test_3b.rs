// Date:   Mon Sep 23 21:33:19 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::time::Duration;
use colored::Colorize;

use super::{Tester, ELECTION_TIMEOUT};

const TIME_LIMIT: Duration = Duration::from_secs(120);

macro_rules! fatal {
    ($($args: expr),*) => {{
        let msg = format!($($args),*).red();
        panic!("{msg}");
    }}
}

#[tokio::test]
async fn test3b_basic_agree() {
    const N: usize = 3;
    const RELIABLE: bool = false;
    const SNAPSHOT: bool = false;

    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;

    tester.begin("Test 3B: basic agreement").await;

    for index in 1..=3 {
        let (nc, _) = tester.n_committed(index).await;
        if nc > 0 {
            fatal!("Some have committed before start");
        }

        let xindex = tester.commit_one(index as u32 * 100, N, false).await;
        if xindex != Some(index) {
            fatal!("Expected {index}, got {xindex:?}");
        }
    }

    tester.end().await;
}

#[tokio::test]
async fn test3b_rpc_byte() {
    const N: usize = 3;
    const RELIABLE: bool = false;
    const SNAPSHOT: bool = false;
    let tester = Tester::<String>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;

    tester.begin("Test 3B: Rpc byte count").await;

    let start_index = match tester.commit_one("0".to_string(), N, false).await {
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
        sent += cmd.len() as u64;
        let xindex = tester.commit_one(cmd, N, false).await;
        if xindex != Some(index) {
            fatal!("Expected {index}, got {xindex:?}");
        }
    }

    let byte1 = tester.byte_cnt().await;
    let bytes_got = byte1 - byte0;
    let byte_expected = sent * N as u64;
    if bytes_got - byte_expected > 5000 {
        fatal!("Too many RPC bytes: got {bytes_got}, expected no more than {}", bytes_got + 5000);
    }
}

/*
 *
 * // test just failure of followers.
func TestFollowerFailure3B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3B): test progressive failure of followers")

	cfg.one(101, servers, false)

	// disconnect one follower from the network.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false)

	// disconnect the remaining follower
	leader2 := cfg.checkOneLeader()
	cfg.disconnect((leader2 + 1) % servers)
	cfg.disconnect((leader2 + 2) % servers)

	// submit a command.
	index, _, ok := cfg.rafts[leader2].Start(104)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}*/
#[tokio::test]
async fn test3b_follower_failure() {
    const N: usize = 3;
    const RELIABLE: bool = false;
    const SNAPSHOT: bool = false;
    let tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;
    tester.begin("Test 3B: test progressive failure of followers").await;

    macro_rules! must_commit {
        ($cmd: expr, $n: expr, $retry: expr) => {
            tester.commit_one(101, N, false).await
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
    let cmd_idx0 = must_commit!(101, N, false);

    // disconnect a follower from the network
    let leader1 = tester.check_one_leader().await;
    tester.disconnect((leader1 + 1) % N).await;

    // the leader and the remaining followers should be able to agree 
    // despite a follower is disconnected.
    let cmd_idx1 = must_commit!(102, N-1, false);
    must_continue!(cmd_idx0, cmd_idx1);

    tokio::time::sleep(ELECTION_TIMEOUT).await;
    let cmd_idx2 = must_commit!(103, N-1, false);
    must_continue!(cmd_idx1, cmd_idx2);

    // disconnect the remaining followers
    let leader2 = tester.check_one_leader().await;
    tester.disconnect((leader2 + 1) % N).await;
    tester.disconnect((leader2 + 2) % N).await;

    let cmd_idx3 = must_commit!(104, 1, false);
    must_continue!(cmd_idx2, cmd_idx3);

    tokio::time::sleep(ELECTION_TIMEOUT * 2).await;

    // cmd_idx3 should not be committed
    let (n, _) = tester.n_committed(cmd_idx3).await;
    if n > 0 {
        fatal!("Command {cmd_idx3} is committed while majority of nodes are disconnected");
    }
}
