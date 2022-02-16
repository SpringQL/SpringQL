// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::net::IpAddr;

use anyhow::Context;

use crate::error::{Result, SpringError};

use super::Options;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum NetProtocol {
    Tcp,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct NetClientOptions {
    pub(crate) protocol: NetProtocol,
    pub(crate) remote_host: IpAddr,
    pub(crate) remote_port: u16,
}

impl TryFrom<&Options> for NetClientOptions {
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

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct NetServerOptions {
    pub(crate) protocol: NetProtocol,
    pub(crate) port: u16,
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
