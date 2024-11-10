// Date:   Thu Aug 29 11:07:14 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::sync::Arc;

use labrpc::client::Client;
use serde::{Deserialize, Serialize};

use crate::{
    debug,
    event::{EvQueue, Event},
    follower::Follower,
    info,
    logs::{LogEntry, Logs},
    persist::{Persister, RaftState},
    role::{Role, RoleCore, RoleEvQueue},
    service::RpcService,
    warn, ApplyMsg, UbRx, UbTx,
};

pub(crate) struct RaftCore {
    pub me: usize,
    pub rpc_client: Client,
    pub persister: Persister,
    pub term: usize,
    pub vote_for: Option<usize>,
    pub apply_ch: UbTx<ApplyMsg>,
}

#[derive(Clone)]
pub(crate) struct RaftHandle {
    pub ev_q: Arc<EvQueue>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RaftInfo {
    term: usize,
    vote_for: Option<usize>,
}

// The Raft object to implement a single raft node.
pub struct Raft {
    pub me: usize,
    ev_q: Arc<EvQueue>,
}

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
    ) -> Self {
        let (ev_ch_tx, ev_ch_rx) = tokio::sync::mpsc::unbounded_channel();
        let ev_q = Arc::new(EvQueue::new(ev_ch_tx, me));

        debug!("Raft[{me}]: lai = {lai:?}");
        let state: RaftState = match raft_state {
            None => {
                let state = Default::default();
                debug!("Raft {me} init from default state");
                debug_assert!(lai.is_none());
                state
            }
            Some(s) => {
                let state = bincode::deserialize_from(&s[..]).unwrap();
                debug!("Raft {me} init from state \n{state:?}");
                state
            }
        };
        let RaftState {
            raft_info,
            logs_info,
        } = state;

        let handle = RaftHandle { ev_q: ev_q.clone() };

        let logs = Logs::new(me, logs_info);

        // apply commands that may get left before crash.
        if let Some(lai) = lai {
            let lai_idx = match logs.index_cmd(lai) {
                Some(idx) => idx,
                None => panic!("{lai} does not exist in logs, that's odd."),
            };
            let lci = logs.lci();
            debug_assert!(
                lai_idx <= lci,
                "lai {lai_idx} should be less than lci {lci}"
            );
            if lai_idx < lci {
                let apply_range = (lai_idx + 1)..=lci;
                for (index, command) in logs
                    .get_range(&apply_range)
                    .unwrap()
                    .into_iter()
                    .cloned()
                    .filter_map(LogEntry::into)
                {
                    if let Err(_) = apply_ch.send(ApplyMsg::Command { index, command }) {
                        warn!("Apply channel should not be closed so quick");
                    }
                }
            }
        }

        rpc_client
            .add_service("RpcService".to_string(), Box::new(RpcService::new(handle)))
            .await;

        let core = RaftCore {
            me,
            rpc_client,
            persister,
            term: raft_info.term,
            vote_for: raft_info.vote_for,
            apply_ch,
        };

        let flw = Follower::from(RoleCore {
            raft_core: core,
            logs,
            ev_q: RoleEvQueue::new(ev_q.clone(), 0),
        });
        tokio::spawn(Self::process_ev(Role::Follower(flw), ev_ch_rx));

        info!("Raft instance {me} started.");

        Self { me, ev_q }
    }

    /// Get the state of this raft node.
    ///
    /// # Retrun
    ///
    /// Returns the term of the raft node and if the node believes
    /// its a leader.
    pub async fn get_state(&self) -> (usize, bool) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::GetState(tx);
        self.ev_q.just_put(ev).unwrap();
        rx.await.unwrap()
    }

    /// In test 3D, the tester may occasionally take a snapshot,
    /// and provide the snapshot to all raft nodes through this
    /// function.
    /// # Arguments
    ///
    /// * `index` - the last log index included in the snapshot.
    /// * `snapshot` - the snapshot bytes.
    ///
    /// # Return
    ///
    /// Nothing.
    pub async fn snapshot(&self, _index: usize, _snapshot: Vec<u8>) {}

    /// Start a Command
    /// If the server believes it's a leader, it should return a
    /// Some((A, B)), where A is the index that the command will appear
    /// at if it's ever committed, B is the current term.
    /// If it does not, it should just return None.
    pub async fn start(&self, command: Vec<u8>) -> Option<(usize, usize)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = Event::StartCmd {
            cmd: command,
            reply_tx: tx,
        };
        self.ev_q.just_put(ev).unwrap();
        rx.await.unwrap()
    }

    /// Kill the server.
    ///
    /// As for Test 3C, you should not persist your state only when killed.
    /// As the tester may lock the persister, and you will fail to persist
    /// your state.
    /// Always persist immediatelly after essential raft state changed.
    pub async fn kill(self) {
        self.ev_q
            .just_put(Event::Kill)
            .expect("Kill ev should not be rejected");
    }

    async fn process_ev(mut role: Role, mut ev_ch_rx: UbRx<Event>) {
        while let Some(ev) = ev_ch_rx.recv().await {
            match ev {
                Event::Kill => {
                    ev_ch_rx.close();
                    role.stop();
                    break;
                }
                ev => role.process(ev).await,
            }
        }
    }
}

impl std::fmt::Display for RaftCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[RaftCore {},term={}]", self.me, self.term)
    }
}

impl std::fmt::Display for Raft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Raft {}]", self.me)
    }
}

impl From<&RaftCore> for RaftInfo {
    fn from(core: &RaftCore) -> Self {
        Self {
            term: core.term,
            vote_for: core.vote_for,
        }
    }
}

impl Serialize for RaftCore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("RaftInfo", 2)?;
        s.serialize_field("term", &self.term)?;
        s.serialize_field("vote_for", &self.vote_for)?;
        s.end()
    }
}
