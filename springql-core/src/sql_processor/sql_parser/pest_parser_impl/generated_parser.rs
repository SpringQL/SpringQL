// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use pest_derive::Parser;

/// The parser generated from `springql.pest`.
///
/// pest_derive::Parser macro puts `pub enum Rule` at this level.
#[derive(Parser)]
#[grammar = "sql_processor/sql_parser/pest_grammar/springql.pest"]
pub(super) struct GeneratedParser;
