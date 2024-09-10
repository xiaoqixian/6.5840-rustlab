### Network

To make the simulation easier, We assume the network group 
members remain the same.
So when creating a `Network` instance, a `size` argument is 
needed to appoint the number of members in the network.

When we let a raft node crash, to make it will not affect 
the current network environment, like sending out-dated 
messages (a crashed server should not be able to send anything
out). 

To achieve that, we make each `Client` carry an unique pass key,
this pass key should also contain in the message it delivers.
```rust 
struct Msg {
    pub key: Key,
    pub from: Idx,
    pub to: Idx,
    pub req: RpcReq,
    pub reply_tx: OneTx<CallResult>
}
```
When the network receives a message, it will look up the current 
pass key of the `from` node, if it match with the key in message, 
then the message is valid, and the request will be dispatched to 
the `to` server.

### Error Forwarding

Many kind of errors are generated when the system is running.
However, only few of them are forwarded, otherwise we'll just 
let the program crash.

For instance, all of the thrid-party libs errors. We assume 
they are robust, so all `Result<T, E>` return types are 
unwrapped immediatelly.

All in all, there're only two kinds of errors:
- Network errors. All of the possible network errors in the 
  simulated network environment. Like `Timeout`, `PeerNotFound`, etc.
- Service errors. RPC service related errors, thrown by RPC servers, 
  like `ClassNotFound`, `MethodNotFound`, etc.


