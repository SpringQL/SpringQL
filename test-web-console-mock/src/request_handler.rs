use std::sync::Arc;

use simple_server::{Builder, Request, ResponseResult, StatusCode};

use crate::request_body::PostTaskGraphBody;

#[derive(Debug)]
pub(super) struct RequestHandler {
    request: Request<Vec<u8>>,
    response: Builder,
}

impl RequestHandler {
    pub(super) fn new(request: Request<Vec<u8>>, response: Builder) -> Self {
        Self { request, response }
    }

    pub(super) fn get_health(&mut self) -> ResponseResult {
        Ok(self.response.body("ok".as_bytes().to_vec())?)
    }

    pub(super) fn not_found(&mut self) -> ResponseResult {
        self.response.status(StatusCode::NOT_FOUND);
        Ok(self.response.body("404 Not found".as_bytes().to_vec())?)
    }

    pub(super) fn post_task_graph(
        &mut self,
        callback: Option<Arc<dyn Fn(PostTaskGraphBody) + Sync + Send>>,
    ) -> ResponseResult {
        let req_body = self.request.body();
        let req_body = String::from_utf8(req_body.to_vec()).unwrap();
        let body: PostTaskGraphBody = serde_json::from_str(req_body.as_str())
            .unwrap_or_else(|_| panic!("failed to parse response: {}", req_body));

        if let Some(cb) = callback {
            cb(body);
        }
        Ok(self.response.body("ok".as_bytes().to_vec())?)
    }
}
