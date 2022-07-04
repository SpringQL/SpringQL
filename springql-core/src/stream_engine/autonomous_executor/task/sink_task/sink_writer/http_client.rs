// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{net::SocketAddr, time::Duration};

use reqwest::{header::HeaderMap, Method, Url};

use crate::{
    api::error::{foreign_info::ForeignInfo, Result, SpringError},
    api::SpringSinkWriterConfig,
    pipeline::{ColumnName, Http1ClientOptions, HttpMethod, Options},
    stream_engine::{
        autonomous_executor::{task::sink_task::sink_writer::SinkWriter, SchemalessRow},
        SqlValue,
    },
};

#[derive(Debug)]
pub struct HttpClientSinkWriter {
    foreign_addr: SocketAddr,

    timeout: Duration,
    connect_timeout: Duration,

    http_method: Method,
    url: Url,
    http_headers: HeaderMap,
    http_body_blob_column: ColumnName,
}
impl From<HttpMethod> for Method {
    fn from(m: HttpMethod) -> Self {
        match m {
            HttpMethod::Post => Method::POST,
        }
    }
}

impl SinkWriter for HttpClientSinkWriter {
    fn start(options: &Options, config: &SpringSinkWriterConfig) -> Result<Self> {
        let options = Http1ClientOptions::try_from(options)?;
        let sock_addr = SocketAddr::new(options.remote_host, options.remote_port);

        let timeout = Duration::from_millis(config.http_timeout_msec as u64);
        let connect_timeout = Duration::from_millis(config.http_connect_timeout_msec as u64);

        let http_method = Method::from(options.method);
        let url = options.url.clone();
        let http_headers =
            HeaderMap::try_from(&options.headers).expect("don't know why this fails");
        let http_body_blob_column = options.blob_body_column;

        log::info!("[HttpClientSinkWriter] Ready to connect {}", sock_addr);

        Ok(Self {
            foreign_addr: sock_addr,
            timeout,
            connect_timeout,
            http_method,
            url,
            http_headers,
            http_body_blob_column,
        })
    }

    fn send_row(&mut self, row: SchemalessRow) -> Result<()> {
        let blob_column = row.get_by_column_name(&self.http_body_blob_column)?;
        if let SqlValue::NotNull(nn_sql_value) = blob_column {
            let body = nn_sql_value.unpack::<Vec<u8>>()?;
            self.send_request(body)
        } else {
            unimplemented!("NULL blob column is not supported yet");
        }
    }
}

impl HttpClientSinkWriter {
    fn send_request(&mut self, body: Vec<u8>) -> Result<()> {
        let req_builder = if self.http_method == Method::POST {
            reqwest::blocking::Client::builder()
                .connect_timeout(self.connect_timeout)
                .timeout(self.timeout)
                .build()
                .expect("msg: failed to create reqwest client")
                .post(self.url.to_string())
        } else {
            unimplemented!("HTTP method {} is not supported yet", self.http_method);
        };

        let content_length = body.len();
        self.http_headers
            .insert("Content-Length", content_length.into());

        let _resp = req_builder
            .headers(self.http_headers.clone())
            .body(body)
            .send()
            .map_err(|e| SpringError::ForeignIo {
                foreign_info: ForeignInfo::Http(self.foreign_addr),
                source: e.into(),
            })?;

        Ok(())
    }
}
