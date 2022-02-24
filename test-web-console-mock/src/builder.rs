// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::{WebConsoleMock, request_body::PostTaskGraphBody};

#[derive(Default)]
pub struct WebConsoleMockBuilder {
    pub(crate) cb_post_pipeline: Option<Arc<dyn Fn(PostTaskGraphBody) + Sync + Send>>,
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
