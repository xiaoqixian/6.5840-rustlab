// Date:   Sun Oct 13 14:55:17 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::Duration};

use super::{
    Tester, timeout_test, ELECTION_TIMEOUT, 
    utils::{gen_bool, randu32, randu64, randusize}
};

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
        let tester = Tester::<u32>::new(N, true, false).await?;
        tester.begin("Test 3C: basic persistence").await;

        let starti = tester.must_submit_cmd(&11, N, true).await?;

        // crash and restart all
        for i in 0..N {
            tester.start_one(i, false, true).await?;
        }

        tester.must_submit_cmd(&12, N, true).await?;

        let leader1 = tester.check_one_leader().await?;
        tester.start_one(leader1, false, true).await?;
        debug!("restarted leader1 {leader1}");

        tester.must_submit_cmd(&13, N, true).await?;

        let leader2 = tester.check_one_leader().await?;
        tester.must_submit_cmd(&14, N-1, true).await?;
        tester.start_one(leader2, false, true).await?;
        debug!("restarted leader2 {leader2}");

        // wait for leader2 to join
        tester.wait(starti+4, N, None).await?;
        
        let leader3 = tester.check_one_leader().await?;
        let i3 = (leader3 + 1) % N;
        tester.disconnect(i3).await;
        tester.must_submit_cmd(&15, N-1, true).await?;
        tester.start_one(i3, false, true).await?;
        tester.connect(i3).await;

        tester.must_submit_cmd(&16, N, true).await?;
        tester.end().await?;
        
        Ok(())
    }
    timeout_test(persist1()).await;
}

#[tokio::test]
async fn test3c_persist2() {
    async fn persist2() -> Result<(), String> {
        const N: usize = 5;
        let tester = Tester::<u32>::new(N, true, false).await?;
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

            tester.start_one((leader1 + 1) % N, false, true).await?;
            tester.start_one((leader1 + 2) % N, false, true).await?;
            tester.connect((leader1 + 1) % N).await;
            tester.connect((leader1 + 2) % N).await;

            tokio::time::sleep(ELECTION_TIMEOUT).await;

            tester.start_one((leader1 + 3) % N, false, true).await?;
            tester.connect((leader1 + 3) % N).await;

            tester.must_submit_cmd(&cmd, N-2, true).await?;
            cmd += 1;
        
            tester.connect(leader1).await;
            tester.connect((leader1 + 4) % N).await;
        }

        tester.must_submit_cmd(&1000, N, true).await?;
        tester.end().await
    }
    timeout_test(persist2()).await;
}

#[tokio::test]
async fn test3c_persist3() {
    async fn persist3() -> Result<(), String> {
        const N: usize = 3;
        let tester = Tester::<u32>::new(N, true, false).await?;
        tester.begin("Test 3C: partitioned leader and one follower crash, leader restarts").await;

        tester.must_submit_cmd(&101, N, true).await?;

        let leader = tester.check_one_leader().await?;
        tester.disconnect((leader + 2) % N).await;

        tester.must_submit_cmd(&102, N-1, true).await?;
        
        tester.crash_some(&[
            leader,
            (leader + 1) % N
        ]).await?;
        tester.connect((leader + 2) % N).await;

        tester.start_one(leader, false, true).await?;

        tester.must_submit_cmd(&103, N-1, true).await?;

        tester.start_one((leader + 1) % N, false, true).await?;
        
        tester.must_submit_cmd(&104, N, true).await?;

        tester.end().await
    }
    timeout_test(persist3()).await;
}

// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
#[tokio::test]
async fn test3c_figure8() {
    async fn figure8() -> Result<(), String> {
        const N: usize = 5;
        const QUORUM: usize = (N + 1) / 2;
        let tester = Tester::<u32>::new(N, true, false).await?;
        tester.begin("Test 3C: figure 8").await;

        let randu32 = rand::random::<u32>;

        tester.must_submit_cmd(&randu32(), 1, true).await?;
        let mut awake = N;
        for _ in 0..1000 {
            let leader = {
                let mut leader = None;
                let config = tester.config.read().await;
                for (id, raft) in config.nodes.iter().enumerate()
                    .filter_map(|(id, node)| node.raft.as_ref().map(|raft| (id, raft)))
                {
                    let cmd = bincode::serialize(&randu32()).unwrap();
                    if raft.start(cmd).await.is_some() {
                        leader = Some(id);
                    }
                }
                leader
            };

            use rand::Rng;
            let ms = if rand::thread_rng().gen_bool(0.1) {
                rand::random::<u64>() % (ELECTION_TIMEOUT.as_millis() as u64 / 2)
            } else {
                rand::random::<u64>() % 13
            };
            tokio::time::sleep(Duration::from_millis(ms)).await;

            if let Some(leader) = leader {
                tester.crash_one(leader).await?;
                awake -= 1;
            }

            if awake < QUORUM {
                let wake = rand::random::<usize>() % N;
                if tester.start_one(wake, false, false).await? {
                    awake += 1;
                }
            }
        }

        for i in 0..N {
            tester.start_one(i, false, false).await?;
        }

        tester.end().await
    }
    timeout_test(figure8()).await;
}

#[tokio::test]
async fn test3c_unreliable_agree() {
    async fn unreliable_agree() -> Result<(), String> {
        const N: usize = 5;
        let tester = Tester::<u32>::new(N, false, false).await?;
        tester.begin("Test 3C: unreliable agreement").await;

        use std::sync::Arc;
        use tokio::task::JoinSet;
        let tester = Arc::new(tester);
        let mut join_set = JoinSet::new();
        for i in 1..50 {
            for j in 0..4 {
                let tester = tester.clone();
                join_set.spawn(async move {
                    tester.submit_cmd(&(100*i + j), 1, true).await?;
                    Ok(())
                });
            }
            let tester = tester.clone();
            tester.submit_cmd(&i, 1, true).await?;
        }
        
        tester.reliable(true).await;
        
        if let Some(e) = join_set.join_all().await
            .into_iter()
            .find(|r| r.is_err())
        {
            return e;
        }

        tester.must_submit_cmd(&100, N, true).await?;
        tester.end().await
    }
    timeout_test(unreliable_agree()).await;
}

#[tokio::test]
async fn test3c_figure8_unreliable() {
    async fn figure8_unreliable() -> Result<(), String> {
        const N: usize = 5;
        const QUORUM: usize = (N + 1) / 2;
        let tester = Tester::<u32>::new(N, false, false).await?;
        tester.begin("Test 3C: figure 8 unreliable").await;

        let gen = |_: usize| randu32() % 10000;

        tester.submit_cmd(&(randu32() % 10000), 1, true).await?;

        let mut awake = N;
        for i in 0..1000 {
            if i == 200 {
                tester.long_reordering(true).await;
            }

            let leader = tester.let_one_start(gen).await
                .map(|(id, _)| id);
            
            let ms = if gen_bool(0.1) {
                randu64() % (ELECTION_TIMEOUT.as_millis() as u64 / 2)
            } else {
                randu64() % 13
            };
            tokio::time::sleep(Duration::from_millis(ms)).await;

            let disconn = randu64() % 1000 < ELECTION_TIMEOUT.as_millis() as u64 / 2;
            if let (Some(leader), true) = (leader, disconn) {
                tester.disconnect(leader).await;
                awake -= 1;
            }

            if awake < QUORUM {
                let wake = randusize() % N;
                if !tester.connected(wake).await {
                    tester.connect(wake).await;
                    awake += 1;
                }
            }
        }

        for i in 0..N {
            tester.connect(i).await;
        }

        tester.must_submit_cmd(&(randu32() % 10000), N, true).await?;
        tester.end().await
    }
    timeout_test(figure8_unreliable()).await;
}

/// Concurrently start multiple workers, 
/// each worker keeps asking all of the nodes to start a command, 
/// then observe the command committed at that command index.
/// At the same time, randomly crash and restart some nodes.
/// Expect that all workers observe a same series of commands.
async fn internal_churn(reliable: bool) -> Result<(), String> {
    const N: usize = 5;
    let tester = Tester::<u32>::new(N, reliable, false).await?;
    tester.begin(
        if reliable {
            "Test 3C: churn"
        } else {
            "Test 3C: unreliable churn"
        }
    ).await;

    struct Context {
        me: usize,
        n: usize,
        flag: Arc<AtomicBool>,
        tester: Tester<u32>
    }

    /// While the flag is true, keep letting raft nodes submit commands.
    /// Then wait for the command to be committed, but not wait forever.
    /// If the command is committed, then push it to Vec that will be returned.
    async fn worker(ctx: Context) -> Result<Vec<u32>, String> {
        let mut ret = Vec::new();
        
        while ctx.flag.load(Ordering::Relaxed) {
            let mut cmd_idx = None;
            let the_cmd = randu32();
            for i in 0..ctx.n {
                if let Some((idx, _)) = ctx.tester.let_it_start(i, || the_cmd).await {
                    cmd_idx = Some(idx);
                }
            }

            if let Some(cmd_idx) = cmd_idx {
                // maybe leader will commit our command, maybe not.
                // but don't wait forever.
                for ms in [10, 20, 50, 100, 200] {
                    let (_, cmd) = ctx.tester.n_committed(cmd_idx).await?;
                    if let Some(cmd) = cmd {
                        if cmd == the_cmd {
                            ret.push(the_cmd);
                        }
                        break
                    }
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
            } else {
                let ms = ctx.me as u64 * 17 + 79;
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
        }
        Ok(ret)
    }

    let start_index = tester.must_submit_cmd(&randu32(), N, true).await?;

    let flag = Arc::new(AtomicBool::new(true));
    let mut join_set = tokio::task::JoinSet::new();
    for i in 0..3 {
        join_set.spawn(worker(Context {
            me: i,
            n: N,
            flag: flag.clone(),
            tester: tester.clone()
        }));
    }

    for _ in 0..20 {
        if gen_bool(0.2) {
            tester.disconnect(randusize() % N).await;
        }

        if gen_bool(0.5) {
            let id = randusize() % N;
            tester.start_one(id, false, false).await?;
            tester.connect(id).await;
        }

        if gen_bool(0.2) {
            tester.crash_one(randusize() % N).await?;
        }

        // Make crash/restart infrequent enough that the peers can often 
        // keep up, but not so infrequent that everything has settled down
        // from the change to the next.
        // Pick a value smaller than the ELECTION_TIMEOUT, but not 
        // hugely smaller.
        tokio::time::sleep(ELECTION_TIMEOUT * 7 / 10).await;
    }

    tokio::time::sleep(ELECTION_TIMEOUT).await;
    tester.reliable(true).await;

    for i in 0..N {
        tester.start_one(i, false, false).await?;
        tester.connect(i).await;
    }

    flag.store(false, Ordering::Relaxed);

    let values = join_set.join_all().await
        .into_iter()
        .collect::<Result<Vec<Vec<u32>>, String>>()?
        .into_iter()
        .flat_map(|vals| vals.into_iter())
        .collect::<Vec<_>>();

    let last_index = tester.must_submit_cmd(&randu32(), N, true).await?;
    
    let mut really_committed = Vec::with_capacity(last_index - start_index + 1);
    for idx in start_index..=last_index {
        let cmd_committed = tester.wait(idx, N, None).await?.unwrap();
        really_committed.push(cmd_committed);
    }

    // iterate all values, make sure one of them is actually committed
    for val in values.into_iter() {
        if !really_committed.contains(&val) {
            return Err(format!("Command {val} is not found in really committed commands"));
        }
    }

    tester.end().await
}

#[tokio::test]
async fn test3c_reliable_churn() {
    timeout_test(internal_churn(true)).await;
}

#[tokio::test]
async fn test3c_unreliable_churn() {
    timeout_test(internal_churn(false)).await;
}
