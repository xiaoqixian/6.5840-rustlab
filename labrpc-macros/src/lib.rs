// Date:   Tue Aug 20 10:14:47 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::fmt::Display;

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, FnArg, Generics, ImplItem, ImplItemFn, ItemImpl, Receiver, Signature, Type, Visibility};

mod attr;

fn err<T, M: Display>(span: Span, msg: M) -> Result<T, syn::Error> {
    Err(syn::Error::new(span, msg))
}

fn good_fn(f: &ImplItemFn) -> Result<Box<Type>, syn::Error> {
    let ImplItemFn { vis, sig, .. } = f;
    let Signature { inputs, .. } = sig;

    match vis {
        Visibility::Public(_) => {},
        _ => return err(
            f.span(),
            "RPC impl functions must be public"
        )
    }
    
    if inputs.len() != 2 {
        return err(
            inputs.span(),
            "The function arguments size is expected to be 2"
        );
    }

    let mut inputs = inputs.into_iter();
    let receiver = inputs.next().unwrap();
    match receiver {
        FnArg::Receiver(Receiver {
            reference: Some((_, None)),
            mutability: None,
            colon_token: None,
            ..
        }) => {},
        _ => return err(
            receiver.span(),
            "The first argument is expected to be &self"
        )
    }

    let arg = inputs.next().unwrap();
    match arg {
        FnArg::Typed(pat_type) => Ok(pat_type.ty.clone()),
        _ => err(
            arg.span(),
            "Are you kidding me?"
        )
    }
}

fn rpc_impl(attr_paths: attr::AttrPaths, input: ItemImpl) -> Result<TokenStream2, syn::Error> {
    let ItemImpl {
        trait_,
        self_ty,
        items: impl_items,
        generics,
        ..
    } = input.clone();

    let Generics {
        lt_token,
        params,
        gt_token,
        where_clause
    } = generics;

    let attr::AttrPaths {
        trait_path,
        res_path,
        err_path
    } = attr_paths;

    if let Some((_, path, _)) = trait_ {
        return err(path.span(), "trait impl is not allowed");
    }

    match self_ty.as_ref() {
        Type::Path(_) => {},
        other => return err(other.span(), 
            "The impled type can only be a type path")
    };
    let mut var_to_call = Vec::with_capacity(impl_items.len());

    for item in impl_items {
        match item {
            ImplItem::Fn(f) => {
                let arg_ty = good_fn(&f)?;
                
                let Signature {
                    ident: fn_ident,
                    asyncness,
                    ..
                } = f.sig;

                let fn_ident_str = fn_ident.to_string();
                let awaitness = match asyncness {
                    None => quote!(),
                    Some(_) => quote!(.await)
                };

                var_to_call.push(quote! {
                    #fn_ident_str => {
                        let arg: #arg_ty = match bincode::deserialize_from(&arg[..]) {
                            Err(_) => return Err(#err_path::INVALID_ARGUMENT),
                            Ok(arg) => arg
                        };
                        let res = self.#fn_ident(arg)#awaitness;
                        Ok(bincode::serialize(&res).unwrap())
                    }
                });
            },
            _ => return err(
                item.span(),
                "Only function is allowed in impl"
            )
        }
    }

    let impl_ = quote! {
        #[async_trait::async_trait]
        impl #lt_token #params #gt_token #trait_path for #self_ty #where_clause {
            async fn call(&self, method: &str, arg: &[u8]) -> #res_path {
                match method {
                    #(#var_to_call),*
                    , _ => Err(#err_path::METHOD_NOT_FOUND)
                }
            }
        }
    };

    Ok(quote! {
        #input
        #impl_
    })
}

#[proc_macro_attribute]
pub fn rpc(attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemImpl);
    let attr_paths = parse_macro_input!(attr as attr::AttrPaths);
    let out = match rpc_impl(attr_paths, input) {
        Ok(t) => t,
        Err(e) => e.to_compile_error()
    };
    TokenStream::from(out)
}
