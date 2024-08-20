// Date:   Tue Aug 20 22:00:17 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use proc_macro2::Span;
use syn::{parse::Parse, punctuated::Punctuated, token::PathSep, Ident, Path, PathSegment};

/// Try to parse attr TokenStream into a Path, 
/// if attr is left empty, set to a default Path.
pub struct TraitPath(pub Path);

impl Parse for TraitPath {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        match Path::parse(input) {
            Ok(path) => Ok(Self(path)),
            Err(_) => Ok(Self(Path {
                leading_colon: None,
                segments: {
                    let mut segments = Punctuated::<PathSegment, PathSep>::new();
                    segments.push(PathSegment {
                        ident: Ident::new("labrpc", Span::call_site()),
                        arguments: syn::PathArguments::None
                    });
                    segments
                }
            }))
        }
    }
}
