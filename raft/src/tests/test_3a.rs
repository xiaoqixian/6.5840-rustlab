// Date:   Thu Aug 29 16:29:03 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::time::Duration;
use super::Tester;

const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::test]
async fn test_3a_initial_election() {
    const N: usize = 3;
    const RELIABLE: bool = false;
    const SNAPSHOT: bool = false;
    let tester = Tester::new(N, RELIABLE, SNAPSHOT).await;

    tester.begin("Test (3A): initial election").await;
    // sleep a while to avoid racing with followers learning 
    // of the election, then check all peers agree on the term.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let term1 = tester.check_terms().await;

    tokio::time::sleep(ELECTION_TIMEOUT * 2).await;
    let term2 = tester.check_terms().await;

    if term1 != term2 {
        println!("Warning: term changed even though there are no failures");
    }

    tester.check_one_leader().await;
    tester.end().await;
}

#[tokio::test]
async fn test_3a_reelection() {
    const N: usize = 3;
    const RELIABLE: bool = false;
    const SNAPSHOT: bool = false;
    let tester = Tester::new(N, RELIABLE, SNAPSHOT).await;

    tester.begin("Test (3A): election after network failure").await;

    let leader1 = tester.check_one_leader().await;
    
    // if leader1 is disconnected, a new leader should be elected.
    tester.disconnect(leader1).await;
    tester.check_one_leader().await;
    
    // if the old leader rejoins, that should not disturb the new 
    // leader. And the old leader should switch to follower.
    tester.connect(leader1).await;
    let leader2 = tester.check_one_leader().await;

    // if there's no quorum, no new leader should be elected.
    tester.disconnect(leader2).await;
    tester.disconnect((leader2 + 1) % N as u32).await;
    tokio::time::sleep(ELECTION_TIMEOUT * 2).await;
    tester.check_no_leader().await;

    // if a quorum arises, it should elect a leader.
    tester.connect((leader2 + 1) % N as u32).await;
    tester.check_one_leader().await;

    tester.end().await;
}

#[tokio::test]
async fn test_3a_many_election() {
    const N: usize = 7;
    const RELIABLE: bool = false;
    const SNAPSHOT: bool = false;
    let tester = Tester::new(N, RELIABLE, SNAPSHOT).await;

    tester.begin("Test (3A): multiple elections").await;

    tester.check_one_leader().await;

    for _ in 0..10 {
        let i1 = rand::random::<u32>() % N as u32;
        let i2 = rand::random::<u32>() % N as u32;
        let i3 = rand::random::<u32>() % N as u32;
        tester.disconnect(i1).await;
        tester.disconnect(i2).await;
        tester.disconnect(i3).await;

        // either the current leader should be alive, 
        // or the remaing four should elect a new one.
        tester.check_one_leader().await;

        tester.connect(i1).await;
        tester.connect(i2).await;
        tester.connect(i3).await;
    }

    tester.check_one_leader().await;

    tester.end().await;
}
