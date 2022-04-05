// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub mod file_type;

mod file_parser;
mod timer;

use self::{file_parser::FileParser, file_type::FileType, timer::Timer};
use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset};
use std::{path::Path, thread, time::Duration};

const SLEEP_DURATION: Duration = Duration::from_micros(10);

/// Generates lines with older timestamp from "now".
///
/// If current line's timestamp is newer than "now", `Iterator::next()` blocks (with sleep).
///
/// If lines in a file is not ordered by timestamp, Timed Stream just generates `line_with_newer_timestamp -> line_with_older_timestamp` in consecutive iterations.
#[derive(Debug)]
pub struct TimedStream {
    timestamp_field: String,
    timer: Timer,
    file_parser: FileParser,
}

impl TimedStream {
    pub fn new<P: AsRef<Path>>(
        file_type: FileType,
        file_path: P,
        timestamp_field: String,
        virt_initial_datetime: DateTime<FixedOffset>,
    ) -> Result<Self> {
        let file_parser = FileParser::new(file_type, file_path)?;
        let timer = Timer::new(virt_initial_datetime.into());
        Ok(Self {
            timestamp_field,
            timer,
            file_parser,
        })
    }
}

impl Iterator for TimedStream {
    type Item = Result<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.file_parser.next().map(|res_json| {
            let json = res_json?;

            let timestamp_s = json.get(&self.timestamp_field).with_context(|| {
                format!(
                    r#"timestamp field "{}" not found in line: {}"#,
                    self.timestamp_field, json
                )
            })?.as_str().with_context(|| {
                format!(
                    r#"timestamp field "{}" is a string in line: {}"#,
                    self.timestamp_field, json
                )
            })?;
            let timestamp =
                DateTime::parse_from_rfc3339(timestamp_s)
                    .with_context(||
                        format!(
                            r#"timestamp field "{}" is not in RFC 3339 format. Correct example: "1996-12-19T16:39:57-08:00""#,
                            timestamp_s
                        )
                    )?;

            while self.timer.virt_current_datetime() < timestamp {
                thread::sleep(SLEEP_DURATION);
            }

            Ok(json)
        })
    }
}
