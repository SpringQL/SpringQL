// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::Context;

use crate::{
    api::error::{Result, SpringError},
    pipeline::{option::Options, NetProtocol},
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct NetServerOptions {
    pub protocol: NetProtocol,
    pub port: u16,
}

impl TryFrom<&Options> for NetServerOptions {
    type Error = SpringError;

    fn try_from(options: &Options) -> Result<Self> {
        Ok(Self {
            protocol: options.get("PROTOCOL", |protocol_str| {
                (protocol_str == "TCP")
                    .then(|| NetProtocol::Tcp)
                    .context("unsupported protocol")
            })?,
            port: options.get("PORT", |remote_port_str| {
                remote_port_str.parse().context("invalid port")
            })?,
        })
    }
}
