// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::{Result, SpringError},
    pipeline::{name::QueueName, option::Options},
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct InMemoryQueueOptions {
    pub queue_name: QueueName,
}

impl TryFrom<&Options> for InMemoryQueueOptions {
    type Error = SpringError;

    fn try_from(options: &Options) -> Result<Self> {
        Ok(Self {
            queue_name: options.get("NAME", |name| Ok(QueueName::new(name.to_string())))?,
        })
    }
}
