// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::{request_body::PostTaskGraphBody, WebConsoleMock};

#[derive(Default)]
pub struct WebConsoleMockBuilder {
    pub cb_post_pipeline: Option<Arc<dyn Fn(PostTaskGraphBody) + Sync + Send>>,
}

impl WebConsoleMockBuilder {
    pub fn add_callback_post_pipeline<F: Fn(PostTaskGraphBody) + Sync + Send + 'static>(
        self,
        callback: F,
    ) -> Self {
        let mut me = self;
        me.cb_post_pipeline = Some(Arc::new(callback));
        me
    }

    pub fn build(self) -> WebConsoleMock {
        WebConsoleMock::new(self)
    }
}
