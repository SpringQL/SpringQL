// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{collections::HashMap, net::IpAddr, str::FromStr};

use anyhow::Context;

use crate::{
    api::error::{Result, SpringError},
    pipeline::{option::Options, ColumnName},
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum HttpMethod {
    Post,
}
impl FromStr for HttpMethod {
    type Err = SpringError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "POST" => Ok(Self::Post),
            _ => Err(SpringError::InvalidOption {
                key: "HTTP_METHOD".to_string(),
                value: s.to_string(),
                source: anyhow::anyhow!("unsupported HTTP method {}", s),
            }),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Http1ClientOptions {
    pub remote_host: IpAddr,
    pub remote_port: u16,
    pub method: HttpMethod,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub blob_body_column: ColumnName,
}

impl TryFrom<&Options> for Http1ClientOptions {
    type Error = SpringError;

    fn try_from(options: &Options) -> Result<Self> {
        let headers = Self::parse_headers(options);

        Ok(Self {
            remote_host: options.get("REMOTE_HOST", |remote_host_str| {
                remote_host_str.parse().context("invalid remote host")
            })?,
            remote_port: options.get("REMOTE_PORT", |remote_port_str| {
                remote_port_str.parse().context("invalid remote port")
            })?,
            method: options.get("METHOD", |method_str| {
                method_str.parse().context("invalid HTTP method")
            })?,
            path: options.get("PATH", |path_str| Ok(path_str.clone()))?,
            headers,
            blob_body_column: options.get("BLOB_BODY_COLUMN", |column_str| {
                Ok(ColumnName::new(column_str.to_string()))
            })?,
        })
    }
}

impl Http1ClientOptions {
    fn parse_headers(options: &Options) -> HashMap<String, String> {
        let mut headers = HashMap::new();

        for (key, value) in options.as_key_values() {
            if key.starts_with("HEADER_") {
                let header_key = key.trim_start_matches("HEADER_").to_string();
                headers.insert(header_key, value.to_string());
            }
        }
        headers
    }
}
