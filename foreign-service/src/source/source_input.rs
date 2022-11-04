// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::collections::VecDeque;

use anyhow::Result;

#[derive(Debug)]
pub enum ForeignSourceInput {
    FifoBatch(VecDeque<serde_json::Value>),
}

impl ForeignSourceInput {
    pub fn new_fifo_batch(input: Vec<serde_json::Value>) -> Self {
        let v = input.into_iter().collect();
        Self::FifoBatch(v)
    }
}

impl Iterator for ForeignSourceInput {
    type Item = Result<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ForeignSourceInput::FifoBatch(batch) => batch.pop_front().map(Ok),
        }
    }
}
