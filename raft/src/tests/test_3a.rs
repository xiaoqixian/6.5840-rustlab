// Date:   Thu Aug 29 16:29:03 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::time::Duration;
use super::Tester;

const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

/*
 *func TestInitialElection3A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (3A): initial election")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
}

 */
#[tokio::test]
async fn test_3a_initial_election() {
    const N: usize = 3;
    const RELIABLE: bool = false;
    const SNAPSHOT: bool = false;
    let tester = Tester::new(N, RELIABLE, SNAPSHOT).await;
}
