mod web_console_request;

use crate::stream_engine::{
    autonomous_executor::{performance_metrics::PerformanceMetrics, task_graph::TaskGraph},
    time::duration::{wall_clock_duration::WallClockDuration, SpringDuration},
};

use self::web_console_request::WebConsoleRequest;

/// Reports performance summary to [web-console API](https://github.com/SpringQL/web-console/blob/main/doc/api.md).
#[derive(Debug)]
pub(super) struct WebConsoleReporter {
    url: String,
    client: reqwest::blocking::Client,
}

impl WebConsoleReporter {
    pub(super) fn new(host: &str, port: u16, timeout: WallClockDuration) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(Some(timeout.as_std().clone()))
            .build()
            .expect("failed to build a reqwest client");

        let url = format!("http://{}:{}/task-graph", host, port);

        Self { url, client }
    }

    pub(super) fn report(&self, metrics: &PerformanceMetrics, graph: &TaskGraph) {
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
            Err(e) => {
                let body = resp.text().unwrap();
                log::warn!("error response from web-console: {:?} - {}", e, body);
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
