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


