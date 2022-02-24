// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub mod timed_stream;

use self::timed_stream::TimedStream;
use anyhow::Result;
use std::collections::VecDeque;

#[derive(Debug)]
pub enum ForeignSourceInput {
    FifoBatch(VecDeque<serde_json::Value>),
    TimedStream(TimedStream),
}

impl ForeignSourceInput {
    pub fn new_fifo_batch(input: Vec<serde_json::Value>) -> Self {
        let v = input.into_iter().collect();
        Self::FifoBatch(v)
    }

    pub fn new_timed_stream(ts: TimedStream) -> Self {
        Self::TimedStream(ts)
    }
}

impl Iterator for ForeignSourceInput {
    type Item = Result<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ForeignSourceInput::FifoBatch(batch) => batch.pop_front().map(Ok),
            ForeignSourceInput::TimedStream(timed_stream) => timed_stream.next(),
        }
    }
}
