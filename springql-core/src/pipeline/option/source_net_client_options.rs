// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::net::IpAddr;

use anyhow::Context;

use crate::{
    api::error::{Result, SpringError},
    pipeline::{option::Options, NetProtocol},
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SourceNetClientOptions {
    pub protocol: NetProtocol,
    pub remote_host: IpAddr,
    pub remote_port: u16,
}

impl TryFrom<&Options> for SourceNetClientOptions {
    type Error = SpringError;

    fn try_from(options: &Options) -> Result<Self> {
        Ok(Self {
            protocol: options.get("PROTOCOL", |protocol_str| {
                (protocol_str == "TCP")
                    .then(|| NetProtocol::Tcp)
                    .context("unsupported protocol")
            })?,
            remote_host: options.get("REMOTE_HOST", |remote_host_str| {
                remote_host_str.parse().context("invalid remote host")
            })?,
            remote_port: options.get("REMOTE_PORT", |remote_port_str| {
                remote_port_str.parse().context("invalid remote port")
            })?,
        })
    }
}
