// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Foreign system information for error reporting.

use std::{
    fmt::{Debug, Display},
    net::SocketAddr,
};

use crate::pipeline::QueueName;

/// Foreign system information for error reporting.
pub enum ForeignInfo {
    /// Generic TCP connection.
    GenericTcp(SocketAddr),

    /// HTTP client
    Http(SocketAddr),

    /// Socket CAN interface
    SocketCAN(String),

    /// In memory queue
    InMemoryQueue(QueueName),
}

impl Display for ForeignInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let detail = match self {
            ForeignInfo::GenericTcp(addr) => format!("TCP connection to {:?}", addr),
            ForeignInfo::Http(addr) => format!("HTTP connection to {:?}", addr),
            ForeignInfo::SocketCAN(interface) => format!("Socket CAN interface {}", interface),
            ForeignInfo::InMemoryQueue(queue_name) => format!("In-memory queue {}", queue_name),
        };

        write!(f, "[foreign info.] {}", detail)
    }
}
impl Debug for ForeignInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}
