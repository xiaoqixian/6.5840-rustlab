// Date:   Fri Aug 16 19:18:51 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

use auto_from::auto_throw;

pub const PEER_NOT_FOUND: Error = Error::NetworkError(NetworkError::PeerNotFound);
pub const DISCONNECTED: Error = Error::NetworkError(NetworkError::Disconnected);
pub const NO_RESPONSE: Error=  Error::NetworkError(NetworkError::NoResponse);
pub const TIMEOUT: Error=  Error::NetworkError(NetworkError::TimeOut);

pub const CLASS_NOT_FOUND: Error = Error::ServiceError(ServiceError::ClassNotFound);
pub const METHOD_NOT_FOUND: Error = Error::ServiceError(ServiceError::MethodNotFound);
pub const INVALID_ARGUMENT: Error = Error::ServiceError(ServiceError::InvalidArgument);

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
}

#[derive(Debug, PartialEq, Eq)]
#[auto_throw]
pub enum Error {
    NetworkError(NetworkError),
    ServiceError(ServiceError)
}
