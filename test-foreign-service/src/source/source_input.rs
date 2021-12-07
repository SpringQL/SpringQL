// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod timed_stream;

use self::timed_stream::TimedStream;
use anyhow::Result;
use std::collections::VecDeque;

#[derive(Debug)]
pub enum TestForeignSourceInput {
    FifoBatch(VecDeque<serde_json::Value>),
    TimedStream(TimedStream),
}

impl TestForeignSourceInput {
    pub fn new_fifo_batch(input: Vec<serde_json::Value>) -> Self {
        let v = input.into_iter().collect();
        Self::FifoBatch(v)
    }

    pub fn new_timed_stream(ts: TimedStream) -> Self {
        Self::TimedStream(ts)
    }
}

impl Iterator for TestForeignSourceInput {
    type Item = Result<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TestForeignSourceInput::FifoBatch(batch) => batch.pop_front().map(Ok),
            TestForeignSourceInput::TimedStream(timed_stream) => timed_stream.next(),
        }
    }
}
