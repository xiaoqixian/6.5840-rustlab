### Election

在选举过程中, candidate 广播自己的基本信息,
```rust
pub(crate) struct RequestVoteArgs {
    pub(crate) id: usize,
    pub(crate) term: usize,
    // the index and term of the last log
    pub(crate) last_log: (usize, usize)
}
```
其中, `last_log` 是最后一个 log 的 index 和 term.
在初始状态下, 所有节点都没有 log, 此时 两者皆为 0.
由于 leader 的 term 都大于0, 所以不会与其它 log 混淆.

