// Date:   Sun Oct 13 14:55:17 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use super::{Tester, timeout_test, ELECTION_TIMEOUT};

macro_rules! debug {
    ($($args: expr),*) => {
        #[cfg(not(feature = "no_test_debug"))]
        {
            let msg = format!("[TEST]: {}", format_args!($($args),*)).truecolor(240, 191, 79);
            println!("{msg}");
        }
    }
}

#[tokio::test]
async fn test3c_persist1() {
    async fn persist1() -> Result<(), String> {
        const N: usize = 3;
        let tester = Tester::<u32>::new(N, false, false).await?;
        tester.begin("Test 3C: basic persistence").await;

        let starti = tester.must_submit_cmd(&11, N, true).await?;

        // crash and restart all
        for i in 0..N {
            tester.start1(i, false).await?;
        }

        tester.must_submit_cmd(&12, N, true).await?;

        let leader1 = tester.check_one_leader().await?;
        tester.start1(leader1, false).await?;
        debug!("restarted leader1 {leader1}");

        tester.must_submit_cmd(&13, N, true).await?;

        let leader2 = tester.check_one_leader().await?;
        tester.must_submit_cmd(&14, N-1, true).await?;
        tester.start1(leader2, false).await?;
        debug!("restarted leader2 {leader2}");

        // wait for leader2 to join
        tester.wait(starti+4, N, None).await?;
        
        let leader3 = tester.check_one_leader().await?;
        let i3 = (leader3 + 1) % N;
        tester.disconnect(i3).await;
        tester.must_submit_cmd(&15, N-1, true).await?;
        tester.start1(i3, false).await?;
        tester.connect(i3).await;

        tester.must_submit_cmd(&16, N, true).await?;
        tester.end().await?;
        
        Ok(())
    }
    timeout_test(persist1()).await;
}

/*func TestPersist23C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers, true)
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1+1)%servers, cfg.applier)
		cfg.start1((leader1+2)%servers, cfg.applier)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		cfg.start1((leader1+3)%servers, cfg.applier)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}*/
#[tokio::test]
async fn test3c_persist2() {
    async fn persist2() -> Result<(), String> {
        const N: usize = 5;
        let tester = Tester::<u32>::new(N, false, false).await?;
        tester.begin("Test 3C: more persistence").await;

        let mut cmd = 11u32;
        for _ in 0..5 {
            tester.must_submit_cmd(&cmd, N, true).await?;
            cmd += 1;

            let leader1 = tester.check_one_leader().await?;
            tester.disconnect((leader1 + 1) % N).await;
            tester.disconnect((leader1 + 2) % N).await;

            tester.must_submit_cmd(&cmd, N-2, true).await?;
            cmd += 1;

            tester.disconnect(leader1).await;
            tester.disconnect((leader1 + 3) % N).await;
            tester.disconnect((leader1 + 4) % N).await;

            tester.start1((leader1 + 1) % N, false).await?;
            tester.start1((leader1 + 2) % N, false).await?;
            tester.connect((leader1 + 1) % N).await;
            tester.connect((leader1 + 2) % N).await;

            tokio::time::sleep(ELECTION_TIMEOUT).await;

            tester.start1((leader1 + 3) % N, false).await?;
            tester.connect((leader1 + 3) % N).await;

            tester.must_submit_cmd(&cmd, N-2, true).await?;
            cmd += 1;
        
            tester.connect(leader1).await;
            tester.connect((leader1 + 4) % N).await;
        }

        tester.must_submit_cmd(&100, N, true).await?;
        tester.end().await?;
        Ok(())
    }
    timeout_test(persist2()).await;
}
