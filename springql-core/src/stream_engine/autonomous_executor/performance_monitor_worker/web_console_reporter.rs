// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod web_console_request;

use crate::stream_engine::{
    autonomous_executor::{
        performance_metrics::PerformanceMetrics,
        performance_monitor_worker::web_console_reporter::web_console_request::WebConsoleRequest,
        task_graph::TaskGraph,
    },
    time::duration::{wall_clock_duration::WallClockDuration, SpringDuration},
};

/// Reports performance summary to [web-console API](https://github.com/SpringQL/web-console/blob/main/doc/api.md).
#[derive(Debug)]
pub struct WebConsoleReporter {
    url: String,
    client: reqwest::blocking::Client,
}

impl WebConsoleReporter {
    pub fn new(host: &str, port: u16, timeout: WallClockDuration) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(Some(*timeout.as_std()))
            .build()
            .expect("failed to build a reqwest client");

        let url = format!("http://{}:{}/task-graph", host, port);

        Self { url, client }
    }

    pub fn report(&self, metrics: &PerformanceMetrics, graph: &TaskGraph) {
        let request = WebConsoleRequest::from_metrics(metrics, graph);

        let res = self.client.post(&self.url).json(&request.to_json()).send();

        match res {
            Ok(resp) => self.handle_response(resp),
            Err(e) => log::warn!("failed to POST metrics to web-console: {:?}", e),
        }
    }

    fn handle_response(&self, resp: reqwest::blocking::Response) {
        let res_status = resp.error_for_status_ref();
        match res_status {
            Ok(_) => log::debug!("successfully POSTed metrics to web-console"),
            Err(e_status) => {
                match resp.text() {
                    Ok(body) => log::warn!("error response from web-console: {:?} - {}", e_status, body),
                    Err(e_resp) => log::warn!("error response (status {}) from web-console but failed to read response body: {:?}", e_status.status().unwrap(), e_resp),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use springql_test_logger::setup_test_logger;

    use crate::stream_engine::time::duration::SpringDuration;

    use super::*;

    #[ignore]
    #[test]
    fn test_report() {
        setup_test_logger();

        let reporter =
            WebConsoleReporter::new("localhost", 8050, WallClockDuration::from_millis(100));
        reporter.report(
            &PerformanceMetrics::fx_split_join(),
            &TaskGraph::fx_split_join(),
        );
    }
}
