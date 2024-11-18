# Raft

In this lab, you need to implement 5 functions of the `Raft` struct.
- `new`: to create a `Raft` instance.
- `get_state`: to get the state of the Raft server.
- `start`: to start a command, you need to make sure the whole cluster is consistent with the command.(test 3b)
- `snapshot`: to take a snapshot.(test 3d)
- `kill`: invoked when the tester let a Raft server crash.
