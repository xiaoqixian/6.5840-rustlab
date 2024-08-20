// Date:   Tue Aug 20 10:14:47 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use std::fmt::Display;

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, FnArg, ImplItem, ImplItemFn, ItemImpl, PatType, Receiver, Signature, Type, Visibility};

fn snake_to_upcamel(input: &str) -> String {
    if input.chars().any(|ch| ch != '_' && !ch.is_ascii_lowercase()) {
        panic!("snake_to_upcamel: input {input} is not of snake case style");
    }
    let mut out = String::new();
    let mut up_flag = true;
    for ch in input.chars() {
        match ch {
            '_' => up_flag = true,
            s if up_flag => out.push(s.to_ascii_uppercase()),
            s => out.push(s)
        }
    }
    out
}

fn err<T, M: Display>(span: Span, msg: M) -> Result<T, syn::Error> {
    Err(syn::Error::new(span, msg))
}

fn good_fn(f: &ImplItemFn) -> Result<PatType, syn::Error> {
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
            sig.span(),
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

    let arg_ty = match inputs.next().unwrap() {
        FnArg::Typed(ty) => ty,
        FnArg::Receiver(_) => 
            panic!("no way the second argument is a receiver")
    };

    Ok(arg_ty.clone())
}

fn rpc_impl(input: ItemImpl) -> Result<TokenStream2, syn::Error> {
    let ItemImpl {
        trait_,
        self_ty,
        items: impl_items,
        generics,
        ..
    } = input.clone();

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
                        let arg = 
                            bincode::deserialize_from(&arg[..]).unwrap();
                        let res = self.#fn_ident(arg)#awaitness;
                        bincode::serialize(&res).unwrap()
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
        impl #generics Service for #self_ty #generics {
            async fn call(&self, method: &str, arg: &[u8]) -> Vec<u8> {
                match method {
                    #(#var_to_call),*
                    , _ => panic!("")
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
pub fn rpc(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemImpl);
    let out = match rpc_impl(input) {
        Ok(t) => t,
        Err(e) => e.to_compile_error()
    };
    TokenStream::from(out)
}

#[cfg(test)]
mod test;
