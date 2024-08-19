// Date:   Fri Aug 16 19:18:51 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use auto_from::auto_throw;
use bincode::Error as BincodeError;
use tokio::sync::{
    oneshot::error::RecvError as OneshotRecvError,
    mpsc::error::SendError as MpscSendError
};

#[derive(Debug, Clone)]
pub enum ServiceError {
    ClassNotFound,
    MethodNotFound,
    InvalidArgument
}

#[derive(Debug)]
pub enum NetworkError {
    NoResponse,
    Disconnected,
    PeerNotFound,
    TimeOut,
    MethError(String),
    ChannelError(String)
}

#[derive(Debug)]
#[auto_throw]
pub enum Error {
    BincodeError(BincodeError),
    NetworkError(NetworkError)
}

macro_rules! from_channel_err {
    ($err_ty: ty) => {
        impl From<$err_ty> for Error {
            fn from(err: $err_ty) -> Self {
                Self::NetworkError(
                    NetworkError::ChannelError(
                        format!("ChannelError: {err:?}")
                    )
                )
            }
        }
    };

    // with generics
    ($err_ty: ty, $($generics: ident),+) => {
        impl<$($generics),+> From<$err_ty> for Error {
            fn from(err: $err_ty) -> Self {
                Self::NetworkError(
                    NetworkError::ChannelError(
                        format!("ChannelError: {err:?}")
                    )
                )
            }
        }
    }
}

from_channel_err!(OneshotRecvError);
from_channel_err!(MpscSendError<T>, T);

