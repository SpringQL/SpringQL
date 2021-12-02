// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    error::{Result, SpringError},
    pipeline::name::QueueName,
};

use super::Options;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct InMemoryQueueServerOptions {
    pub(crate) queue_name: QueueName,
}

impl TryFrom<&Options> for InMemoryQueueServerOptions {
    type Error = SpringError;

    fn try_from(options: &Options) -> Result<Self> {
        Ok(Self {
            queue_name: options.get("NAME", |name| Ok(QueueName::new(name.to_string())))?,
        })
    }
}
