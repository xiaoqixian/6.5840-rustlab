// Date:   Sat May 18 22:53:01 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

trait RpcService {}
trait RpcArgs {}
trait RpcReply {}

type RpcFunc = dyn Fn(&dyn RpcService, &mut dyn RpcArgs) -> Box<dyn RpcReply>;

fn main() {

}
