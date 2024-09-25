// Date:   Mon Sep 23 21:33:19 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::time::Duration;
use colored::Colorize;

use crate::tests::Tester;

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

/*
 * func TestRPCBytes3B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3B): RPC byte count")

	cfg.one(99, servers, false)
	bytes0 := cfg.bytesTotal()

	iters := 10
	var sent int64 = 0
	for index := 2; index < iters+2; index++ {
		cmd := randstring(5000)
		xindex := cfg.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}*/
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
