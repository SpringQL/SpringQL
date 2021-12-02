// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Foreign system information for error reporting.

use std::{
    fmt::{Debug, Display},
    net::SocketAddr,
};

/// Foreign system information for error reporting.
pub enum ForeignInfo {
    /// Generic TCP connection.
    GenericTcp(SocketAddr),
}

impl Display for ForeignInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let detail = match self {
            ForeignInfo::GenericTcp(addr) => format!("TCP connection to {:?}", addr),
        };

        write!(f, "[foreign info.] {}", detail)
    }
}
impl Debug for ForeignInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}
