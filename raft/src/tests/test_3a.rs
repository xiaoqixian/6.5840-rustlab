// Date:   Thu Aug 29 16:29:03 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::time::Duration;

use super::{Tester, ELECTION_TIMEOUT, timeout_test};

use colored::Colorize;
macro_rules! warn {
    ($($args: expr),*) => {{
        let msg = format_args!($($args),*);
        let msg = format!("[TEST WARNING] {msg}").truecolor(255,175,0);
        println!("{msg}");
    }}
}
macro_rules! debug {
    ($($args: expr),*) => {
        #[cfg(not(feature = "no_test_debug"))]
        {
            let msg = format!("[TEST] {}", format_args!($($args),*))
                .truecolor(240, 191, 79);
            println!("{msg}");
        }
    }
}

#[tokio::test]
async fn test3a_initial_election() {
    async fn initial_election() -> Result<(), String> {
        const N: usize = 3;
        const RELIABLE: bool = false;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;

        tester.begin("Test (3A): initial election").await;
        // sleep a while to avoid racing with followers learning 
        // of the election, then check all peers agree on the term.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let term1 = tester.check_terms().await?;

        tokio::time::sleep(ELECTION_TIMEOUT * 2).await;
        let term2 = tester.check_terms().await?;

        if term1 != term2 {
            warn!("Warning: term changed even though there are no failures");
        }

        tester.check_one_leader().await?;

        tester.end().await?;
        Ok(())
    }
    
    timeout_test(initial_election()).await;
}

#[tokio::test]
async fn test3a_reelection() {
    async fn reelction() -> Result<(), String> {
        const N: usize = 3;
        const RELIABLE: bool = false;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;

        tester.begin("Test (3A): election after network failure").await;

        let leader1 = tester.check_one_leader().await?;
        
        // if leader1 is disconnected, a new leader should be elected.
        tester.disconnect(leader1).await;
        tester.check_one_leader().await?;
        
        // if the old leader rejoins, that should not disturb the new 
        // leader. And the old leader should switch to follower.
        tester.connect(leader1).await;
        let leader2 = tester.check_one_leader().await?;

        // if there's no quorum, no new leader should be elected.
        tester.disconnect(leader2).await;
        tester.disconnect((leader2 + 1) % N).await;
        tokio::time::sleep(ELECTION_TIMEOUT * 2).await;
        tester.check_no_leader().await?;

        // if a quorum arises, it should elect a leader.
        tester.connect((leader2 + 1) % N).await;
        tester.check_one_leader().await?;

        tester.end().await?;
        Ok(())
    }
    timeout_test(reelction()).await;
}

#[tokio::test]
async fn test3a_many_elections() {
    async fn many_elections() -> Result<(), String> {
        const N: usize = 7;
        const RELIABLE: bool = false;
        const SNAPSHOT: bool = false;
        let mut tester = Tester::<u32>::new(N, RELIABLE, SNAPSHOT).await?;

        tester.begin("Test (3A): multiple elections").await;

        tester.check_one_leader().await?;

        for _i in 0..10 {
            debug!("===== Round {_i} ======");
            let i1 = rand::random::<usize>() % N;
            let i2 = rand::random::<usize>() % N;
            let i3 = rand::random::<usize>() % N;
            debug!("-- disconnected {i1}, {i2}, {i2}");
            tester.disconnect(i1).await;
            tester.disconnect(i2).await;
            tester.disconnect(i3).await;

            // either the current leader should be alive, 
            // or the remaing four should elect a new one.
            let _leader = tester.check_one_leader().await?;
            debug!("Round {_i} leader = {_leader}");

            tester.connect(i1).await;
            tester.connect(i2).await;
            tester.connect(i3).await;
        }

        tester.check_one_leader().await?;

        tester.end().await?;
        Ok(())
    }
    timeout_test(many_elections()).await;
}
