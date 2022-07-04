// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::net::IpAddr;

use anyhow::{anyhow, Context};

use crate::{
    api::error::{Result, SpringError},
    pipeline::{option::Options, ColumnName, NetProtocol},
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum SinkFormat {
    Json,
    Blob {
        /// From which column of a sink stream to get the blob data.
        blob_column: ColumnName,
    },
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SinkNetClientOptions {
    pub protocol: NetProtocol,
    pub remote_host: IpAddr,
    pub remote_port: u16,
    pub format: SinkFormat,
}

impl TryFrom<&Options> for SinkNetClientOptions {
    type Error = SpringError;

    fn try_from(options: &Options) -> Result<Self> {
        let format = options
            .get("FORMAT", |format_str| match format_str.as_str() {
                "JSON" => Ok(SinkFormat::Json),
                "BLOB" => Ok(SinkFormat::Blob {
                    blob_column: options.get("BLOB_COLUMN", |blob_column_str| {
                        Ok(ColumnName::new(blob_column_str.clone()))
                    })?,
                }),
                _ => Err(anyhow!("NET_CLIENT sink writer got invalid FORMAT")),
            })
            .unwrap_or_else(|_| {
                log::info!("NET_CLIENT sink writer uses default JSON format");
                SinkFormat::Json
            });

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
            format,
        })
    }
}
