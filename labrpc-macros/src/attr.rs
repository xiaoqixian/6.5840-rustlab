// Date:   Tue Aug 20 22:00:17 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use proc_macro2::Span;
use syn::{parse::Parse, punctuated::Punctuated, token::PathSep, Ident, Path, PathSegment, Token};

/// Try to parse attr TokenStream into a Path, 
/// if attr is left empty, set to a default Path.
pub struct AttrPaths {
    pub trait_path: Path,
    pub res_path: Path,
    pub err_path: Path // MethodNotFound error path
}

fn create_path(path: &str) -> Path {
    const INVALID: &[char] = &['<', '>', '(', ')'];
    if path.chars().any(|c| INVALID.contains(&c)) {
        panic!("Angle brackets or parenthesis is not supported");
    }
    
    let segments = path.split("::").into_iter()
        .map(|seg| PathSegment {
            ident: Ident::new(seg, Span::call_site()),
            arguments: syn::PathArguments::None
        })
        .collect::<Punctuated::<PathSegment, PathSep>>();
    Path {
        leading_colon: None,
        segments
    }
}

impl Parse for AttrPaths {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let empty = input.is_empty();
        match Punctuated::<Path, Token![,]>::parse_terminated(input) {
            Ok(mut paths) if !empty => {
                if paths.len() != 3 {
                    return Err(syn::Error::new(input.span(), "The attributes size is expected to be 3"));
                }
                Ok(AttrPaths {
                    err_path: paths.pop().unwrap().into_value(),
                    res_path: paths.pop().unwrap().into_value(),
                    trait_path: paths.pop().unwrap().into_value(),
                })
            },
            _ => Ok(AttrPaths {
                trait_path: create_path("labrpc::Service"),
                res_path: create_path("labrpc::service::CallResult"),
                err_path: create_path("labrpc::err::ServiceError"),
            })
        }
    }
}
