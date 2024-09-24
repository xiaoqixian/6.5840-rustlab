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
/*
 * func TestBasicAgree3B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3B): basic agreement")

	// iters := 3
	iters := 1
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}
 */
#[tokio::test]
async fn test3b_basic_agree() {
    const N: usize = 3;
    const RELIABLE: bool = false;
    const SNAPSHOT: bool = false;

    let tester = Tester::new(N, RELIABLE, SNAPSHOT, TIME_LIMIT).await;

    tester.begin("Test 3B: basic agreement").await;

    for index in 1..=3 {
        let nc = tester.n_committed(index).await;
        if nc > 0 {
            fatal!("Some have committed before start");
        }
    }
}
