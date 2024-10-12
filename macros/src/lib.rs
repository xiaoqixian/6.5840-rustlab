// Date:   Tue Aug 20 10:14:47 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemImpl};

mod attr;
mod rpc;


#[proc_macro_attribute]
pub fn rpc(attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemImpl);
    let attr_paths = parse_macro_input!(attr as attr::AttrPaths);
    let out = match rpc::rpc_impl(attr_paths, input) {
        Ok(t) => t,
        Err(e) => e.to_compile_error()
    };
    TokenStream::from(out)
}
