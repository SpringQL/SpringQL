// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::Result;
use std::{fs::File, path::Path};

use super::file_type::FileType;

#[derive(Debug)]
pub(super) enum FileParser {
    /// Fields are parsed as the following priority:
    ///
    /// 1. number
    /// 2. string
    Tsv(csv::Reader<File>),
}

impl FileParser {
    pub(super) fn new<P: AsRef<Path>>(file_type: FileType, file_path: P) -> Result<Self> {
        match file_type {
            FileType::Tsv => {
                let reader = csv::ReaderBuilder::new()
                    .delimiter(b'\t')
                    .has_headers(true)
                    .from_path(file_path)?;
                Ok(FileParser::Tsv(reader))
            }
        }
    }

    fn parse_value(v: &str) -> serde_json::Value {
        if let Ok(i) = v.parse::<i64>() {
            serde_json::Value::Number(i.into())
        } else if let Ok(f) = v.parse::<f64>() {
            if let Some(number) = serde_json::Number::from_f64(f) {
                serde_json::Value::Number(number)
            } else {
                serde_json::Value::String(v.to_string())
            }
        } else {
            serde_json::Value::String(v.to_string())
        }
    }
}

impl Iterator for FileParser {
    type Item = Result<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FileParser::Tsv(reader) => reader.records().next().map(|records| {
                let records = records?;
                assert_eq!(records.len(), reader.headers()?.len());

                let mut m = serde_json::Map::new();
                for (field, value) in reader.headers()?.iter().zip(records.iter()) {
                    m.insert(field.to_string(), Self::parse_value(value));
                }

                Ok(serde_json::Value::Object(m))
            }),
        }
    }
}
