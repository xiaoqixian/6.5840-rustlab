// Date:   Thu Aug 29 11:07:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use labrpc::client::Client;

use crate::{
    persist::Persister,
    ApplyMsg, UbTx,
};


// The Raft object to implement a single raft node.
pub struct Raft {}

/// Raft implementation.
/// Most API are marked as async functions, and they will called
/// in async way. So you should not modify the asyncness of a function.
/// If you don't need the function to async, just don't call any other
/// async function in the function body, they will just be like a normal
/// function.
/// Even though, as this lab is designed in async way. You may want to
/// use as much async functions as you can, so your code can run in the
/// best way. For example, use the async waitable lock to replace the
/// std lock.
impl Raft {
    /// To create a new raft node.
    ///
    /// If persister.raft_state().await.is_some() or
    /// persister.snapshot().await.is_some(),
    /// you are supposed to recover raft from the persist data.
    ///
    /// # Arguments
    ///
    /// * `rpc_client` - the RPC client that can be used to add RPC services
    /// and dial RPC requests to peers.
    /// * `me` - the id of this raft node.
    /// * `persister` - a persister is used to persist the state of the
    /// raft node, so the node can recover itself from a crash and restart.
    /// * `apply_ch` - when a command is confirmed as committed, the raft
    /// node can apply it by sending it to the apply channel.
    /// * `lai` - last applied command index, the Applier promises that
    /// all commands that are sent through `apply_ch` successfully will
    /// be applied,
    /// * `raft_state` -
    pub async fn new(
        rpc_client: Client,
        me: usize,
        persister: Persister,
        apply_ch: UbTx<ApplyMsg>,
        lai: Option<usize>,
        raft_state: Option<Vec<u8>>,
        snapshot: Option<Vec<u8>>
    ) -> Self {
        Self {}
    }

    /// Get the state of this raft node.
    ///
    /// # Retrun
    ///
    /// Returns the term of the raft node and if the node believes
    /// its a leader.
    pub async fn get_state(&self) -> (usize, bool) {
        Default::default()
    }

    /// In test 3D, the tester may occasionally take a snapshot,
    /// and provide the snapshot to all raft nodes through this
    /// function.
    /// # Arguments
    ///
    /// * `index` - the last command index included in the snapshot.
    /// * `snapshot` - the snapshot bytes.
    ///
    /// # Return
    ///
    /// Nothing.
    pub async fn snapshot(&self, index: usize, snapshot: Vec<u8>) {}

    /// Start a Command
    /// If the server believes it's a leader, it should return a
    /// Some((A, B)), where A is the index that the command will appear
    /// at if it's ever committed, B is the current term.
    /// If it does not, it should just return None.
    pub async fn start(&self, command: Vec<u8>) -> Option<(usize, usize)> {
        None
    }

    /// Kill the server.
    ///
    /// As for Test 3C, you should not persist your state only when killed.
    /// As the tester may lock the persister, and you will fail to persist
    /// your state.
    /// Always persist immediatelly after essential raft state changed.
    pub async fn kill(self) {}
}
