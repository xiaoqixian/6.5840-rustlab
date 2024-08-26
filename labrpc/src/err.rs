// Date:   Fri Aug 16 19:18:51 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use auto_from::auto_throw;
use tokio::sync::{
    oneshot::error::RecvError as OneshotRecvError,
    mpsc::error::SendError as MpscSendError
};

pub const PEER_NOT_FOUND: Error = Error::NetworkError(NetworkError::PeerNotFound);
pub const DISCONNECTED: Error = Error::NetworkError(NetworkError::Disconnected);
pub const NO_RESPONSE: Error=  Error::NetworkError(NetworkError::NoResponse);
pub const TIMEOUT: Error=  Error::NetworkError(NetworkError::TimeOut);

pub const CLASS_NOT_FOUND: Error = Error::ServiceError(ServiceError::ClassNotFound);
pub const METHOD_NOT_FOUND: Error = Error::ServiceError(ServiceError::MethodNotFound);

#[derive(Debug)]
pub struct BincodeError(bincode::Error);
impl PartialEq for BincodeError {
    fn eq(&self, other: &Self) -> bool {
        use bincode::ErrorKind;

        match (self.0.as_ref(), other.0.as_ref()) {
            (ErrorKind::Io(_), ErrorKind::Io(_)) => true,
            (ErrorKind::InvalidUtf8Encoding(_), ErrorKind::InvalidUtf8Encoding(_)) => true,
            (ErrorKind::InvalidBoolEncoding(_), ErrorKind::InvalidBoolEncoding(_)) => true,
            (ErrorKind::InvalidCharEncoding, ErrorKind::InvalidCharEncoding) => true,
            (ErrorKind::InvalidTagEncoding(_), ErrorKind::InvalidTagEncoding(_)) => true,
            (ErrorKind::DeserializeAnyNotSupported, ErrorKind::DeserializeAnyNotSupported) => true,
            (ErrorKind::SizeLimit, ErrorKind::SizeLimit) => true,
            (ErrorKind::SequenceMustHaveLength, ErrorKind::SequenceMustHaveLength) => true,
            (ErrorKind::Custom(_), ErrorKind::Custom(_)) => true,
            _ => false
        }
    }
}
impl Eq for BincodeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceError {
    ClassNotFound,
    MethodNotFound,
    InvalidArgument
}

#[derive(Debug, PartialEq, Eq)]
pub enum NetworkError {
    NoResponse,
    Disconnected,
    PeerNotFound,
    TimeOut,
    MethError(String),
    ChannelError(String)
}

#[derive(Debug, PartialEq, Eq)]
#[auto_throw]
pub enum Error {
    BincodeError(BincodeError),
    NetworkError(NetworkError),
    ServiceError(ServiceError)
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

impl From<bincode::Error> for Error {
    fn from(value: bincode::Error) -> Self {
        Self::BincodeError(BincodeError(value))
    }
}
