// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod generated_parser;
mod helper;

use crate::error::{Result, SpringError};
use crate::pipeline::foreign_stream_model::ForeignStreamModel;
use crate::pipeline::name::{ColumnName, PumpName, SinkWriterName, SourceReaderName, StreamName};
use crate::pipeline::option::options_builder::OptionsBuilder;
use crate::pipeline::pump_model::pump_state::PumpState;
use crate::pipeline::relation::column::column_constraint::ColumnConstraint;
use crate::pipeline::relation::column::column_data_type::ColumnDataType;
use crate::pipeline::relation::column::column_definition::ColumnDefinition;
use crate::pipeline::relation::sql_type::SqlType;
use crate::pipeline::sink_writer::sink_writer_type::SinkWriterType;
use crate::pipeline::sink_writer::SinkWriter;
use crate::pipeline::source_reader_model::source_reader_type::SourceReaderType;
use crate::pipeline::source_reader_model::SourceReaderModel;
use crate::pipeline::stream_model::stream_shape::StreamShape;
use crate::pipeline::stream_model::StreamModel;
use crate::sql_processor::sql_parser::syntax::{
    ColumnConstraintSyntax, OptionSyntax, SelectStreamSyntax,
};
use crate::stream_engine::command::alter_pipeline_command::AlterPipelineCommand;
use crate::stream_engine::command::insert_plan::InsertPlan;
use crate::stream_engine::command::Command;
use anyhow::{anyhow, Context};
use generated_parser::{GeneratedParser, Rule};
use helper::{parse_child, parse_child_seq, self_as_str, try_parse_child, FnParseParams};
use pest::{iterators::Pairs, Parser};
use std::convert::identity;
use std::sync::Arc;

use super::parse_success::ParseSuccess;

#[derive(Debug, Default)]
pub(super) struct PestParserImpl;

impl PestParserImpl {
    pub(super) fn parse<S: Into<String>>(&self, sql: S) -> Result<ParseSuccess> {
        let sql = sql.into();

        let pairs: Pairs<Rule> = GeneratedParser::parse(Rule::command, &sql)
            .context("failed to parse SQL")
            .map_err(SpringError::Sql)?;

        let mut params = FnParseParams {
            sql: &sql,
            children_pairs: pairs.collect(),
            self_string: sql.clone(),
        };

        parse_child(&mut params, Rule::command, Self::parse_command, identity)
    }

    /*
     * ================================================================================================
     * Lexical Structure:
     * ================================================================================================
     */

    /*
     * ----------------------------------------------------------------------------
     * Constants
     * ----------------------------------------------------------------------------
     */

    fn parse_string_constant(mut params: FnParseParams) -> Result<String> {
        parse_child(
            &mut params,
            Rule::string_content,
            Self::parse_string_content,
            identity,
        )
    }

    fn parse_string_content(mut params: FnParseParams) -> Result<String> {
        let s = self_as_str(&mut params);
        Ok(s.into())
    }

    /*
     * ================================================================================================
     * Commands:
     * ================================================================================================
     */

    fn parse_command(mut params: FnParseParams) -> Result<ParseSuccess> {
        try_parse_child(
            &mut params,
            Rule::create_source_stream_command,
            Self::parse_create_source_stream_command,
            identity,
        )?
        .or(try_parse_child(
            &mut params,
            Rule::create_sink_stream_command,
            Self::parse_create_sink_stream_command,
            identity,
        )?)
        .or(try_parse_child(
            &mut params,
            Rule::create_pump_command,
            Self::parse_create_pump_command,
            identity,
        )?)
        .or(try_parse_child(
            &mut params,
            Rule::alter_pump_command,
            Self::parse_alter_pump_command,
            identity,
        )?)
        .ok_or_else(|| {
            SpringError::Sql(anyhow!(
                "Does not match any child rule of command: {}",
                params.sql
            ))
        })
    }

    /*
     * ----------------------------------------------------------------------------
     * CREATE SOURCE STREAM
     * ----------------------------------------------------------------------------
     */

    fn parse_create_source_stream_command(mut params: FnParseParams) -> Result<ParseSuccess> {
        let foreign_stream_name = parse_child(
            &mut params,
            Rule::stream_name,
            Self::parse_stream_name,
            identity,
        )?;
        let column_definitions = parse_child_seq(
            &mut params,
            Rule::column_definition,
            &Self::parse_column_definition,
            &identity,
        )?;
        let source_reader_name = parse_child(
            &mut params,
            Rule::source_reader_name,
            Self::parse_source_reader_name,
            identity,
        )?;
        let option_syntaxes = try_parse_child(
            &mut params,
            Rule::option_specifications,
            &Self::parse_option_specifications,
            &identity,
        )?;

        let stream_shape = StreamShape::new(column_definitions)?;
        let foreign_stream = ForeignStreamModel::new(StreamModel::new(
            foreign_stream_name,
            Arc::new(stream_shape),
        ));

        let mut options = OptionsBuilder::default();
        if let Some(option_syntaxes) = option_syntaxes {
            for o in option_syntaxes {
                options = options.add(o.option_name, o.option_value);
            }
        }
        let options = options.build();

        let source_reader_type = match source_reader_name.as_ref() {
            "NET_SERVER" => Ok(SourceReaderType::Net),
            _ => Err(SpringError::Sql(anyhow!(
                "Invalid source reader name: {}",
                source_reader_name
            ))),
        }?;
        let source = SourceReaderModel::new(source_reader_type, Arc::new(foreign_stream), options);

        Ok(ParseSuccess::CommandWithoutQuery(Command::AlterPipeline(
            AlterPipelineCommand::CreateForeignSourceStream(source),
        )))
    }

    /*
     * ----------------------------------------------------------------------------
     * CREATE SINK STREAM
     * ----------------------------------------------------------------------------
     */

    fn parse_create_sink_stream_command(mut params: FnParseParams) -> Result<ParseSuccess> {
        let foreign_stream_name = parse_child(
            &mut params,
            Rule::stream_name,
            Self::parse_stream_name,
            identity,
        )?;
        let column_definitions = parse_child_seq(
            &mut params,
            Rule::column_definition,
            &Self::parse_column_definition,
            &identity,
        )?;
        let sink_writer_name = parse_child(
            &mut params,
            Rule::sink_writer_name,
            Self::parse_sink_writer_name,
            identity,
        )?;
        let option_syntaxes = try_parse_child(
            &mut params,
            Rule::option_specifications,
            &Self::parse_option_specifications,
            &identity,
        )?;

        let stream_shape = StreamShape::new(column_definitions)?;
        let foreign_stream = ForeignStreamModel::new(StreamModel::new(
            foreign_stream_name,
            Arc::new(stream_shape),
        ));

        let mut options = OptionsBuilder::default();
        if let Some(option_syntaxes) = option_syntaxes {
            for o in option_syntaxes {
                options = options.add(o.option_name, o.option_value);
            }
        }
        let options = options.build();

        let sink_writer_type = match sink_writer_name.as_ref() {
            "NET_SERVER" => Ok(SinkWriterType::Net),
            "IN_MEMORY_QUEUE" => Ok(SinkWriterType::InMemoryQueue),
            _ => Err(SpringError::Sql(anyhow!(
                "Invalid sink writer name: {}",
                sink_writer_name
            ))),
        }?;
        let sink = SinkWriter::new(sink_writer_type, Arc::new(foreign_stream), options);

        Ok(ParseSuccess::CommandWithoutQuery(Command::AlterPipeline(
            AlterPipelineCommand::CreateForeignSinkStream(sink),
        )))
    }

    /*
     * ----------------------------------------------------------------------------
     * CREATE PUMP
     * ----------------------------------------------------------------------------
     */

    fn parse_create_pump_command(mut params: FnParseParams) -> Result<ParseSuccess> {
        let pump_name = parse_child(
            &mut params,
            Rule::pump_name,
            Self::parse_pump_name,
            identity,
        )?;
        let into_stream = parse_child(
            &mut params,
            Rule::stream_name,
            &Self::parse_stream_name,
            &identity,
        )?;
        let insert_column_names = parse_child_seq(
            &mut params,
            Rule::column_name,
            &Self::parse_column_name,
            &identity,
        )?;
        let select_stream_syntax = parse_child(
            &mut params,
            Rule::select_stream_command,
            Self::parse_select_stream,
            identity,
        )?;

        Ok(ParseSuccess::CreatePump {
            pump_name,
            select_stream_syntax,
            insert_plan: InsertPlan::new(into_stream, insert_column_names),
        })
    }

    /*
     * ----------------------------------------------------------------------------
     * ALTER PUMP
     * ----------------------------------------------------------------------------
     */

    fn parse_alter_pump_command(mut params: FnParseParams) -> Result<ParseSuccess> {
        let pump_name = parse_child(
            &mut params,
            Rule::pump_name,
            &Self::parse_pump_name,
            &identity,
        )?;
        Ok(ParseSuccess::CommandWithoutQuery(Command::AlterPipeline(
            AlterPipelineCommand::AlterPump {
                name: pump_name,
                state: PumpState::Started,
            },
        )))
    }

    /*
     * ----------------------------------------------------------------------------
     * SELECT
     * ----------------------------------------------------------------------------
     */

    fn parse_select_stream(mut params: FnParseParams) -> Result<SelectStreamSyntax> {
        let column_names = parse_child_seq(
            &mut params,
            Rule::column_name,
            &Self::parse_column_name,
            &identity,
        )?;
        let stream_name = parse_child(
            &mut params,
            Rule::stream_name,
            Self::parse_stream_name,
            identity,
        )?;
        Ok(SelectStreamSyntax {
            column_names,
            from_stream: stream_name,
        })
    }

    /*
     * ================================================================================================
     * Identifier:
     * ================================================================================================
     */

    fn parse_identifier(mut params: FnParseParams) -> Result<String> {
        let s = self_as_str(&mut params);
        Ok(s.to_string())
    }

    /*
     * ================================================================================================
     * Data Types:
     * ================================================================================================
     */

    fn parse_data_type(mut params: FnParseParams) -> Result<SqlType> {
        try_parse_child(
            &mut params,
            Rule::integer_type,
            Self::parse_integer_type,
            identity,
        )?
        .or(try_parse_child(
            &mut params,
            Rule::character_type,
            Self::parse_character_type,
            identity,
        )?)
        .or(try_parse_child(
            &mut params,
            Rule::timestamp_type,
            Self::parse_timestamp_type,
            identity,
        )?)
        .ok_or_else(|| {
            SpringError::Sql(anyhow!(
                "Does not match any child rule of data type: {}",
                params.sql
            ))
        })
    }

    /*
     * ----------------------------------------------------------------------------
     * Integer Types
     * ----------------------------------------------------------------------------
     */

    fn parse_integer_type(mut params: FnParseParams) -> Result<SqlType> {
        let s = self_as_str(&mut params);
        match s.to_ascii_uppercase().as_str() {
            "INTEGER" => Ok(SqlType::integer()),
            x => {
                eprintln!("Unexpected data type parsed: {}", x);
                unreachable!();
            }
        }
    }

    fn parse_character_type(mut params: FnParseParams) -> Result<SqlType> {
        let s = self_as_str(&mut params);
        match s.to_ascii_uppercase().as_str() {
            "TEXT" => Ok(SqlType::text()),
            x => {
                eprintln!("Unexpected data type parsed: {}", x);
                unreachable!();
            }
        }
    }

    fn parse_timestamp_type(mut params: FnParseParams) -> Result<SqlType> {
        let s = self_as_str(&mut params);
        match s.to_ascii_uppercase().as_str() {
            "TIMESTAMP" => Ok(SqlType::timestamp()),
            x => {
                eprintln!("Unexpected data type parsed: {}", x);
                unreachable!();
            }
        }
    }

    /*
     * ================================================================================================
     * Misc:
     * ================================================================================================
     */

    /*
     * ----------------------------------------------------------------------------
     * Names
     * ----------------------------------------------------------------------------
     */

    fn parse_stream_name(mut params: FnParseParams) -> Result<StreamName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            StreamName::new,
        )
    }

    fn parse_pump_name(mut params: FnParseParams) -> Result<PumpName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            PumpName::new,
        )
    }

    fn parse_source_reader_name(mut params: FnParseParams) -> Result<SourceReaderName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            SourceReaderName::new,
        )
    }
    fn parse_sink_writer_name(mut params: FnParseParams) -> Result<SinkWriterName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            SinkWriterName::new,
        )
    }

    fn parse_column_name(mut params: FnParseParams) -> Result<ColumnName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            ColumnName::new,
        )
    }

    fn parse_option_name(mut params: FnParseParams) -> Result<String> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            identity,
        )
    }

    fn parse_option_value(mut params: FnParseParams) -> Result<String> {
        parse_child(
            &mut params,
            Rule::string_constant,
            Self::parse_string_constant,
            identity,
        )
    }

    /*
     * ----------------------------------------------------------------------------
     * Constraints
     * ----------------------------------------------------------------------------
     */

    fn parse_column_constraint(mut params: FnParseParams) -> Result<ColumnConstraintSyntax> {
        let s = self_as_str(&mut params);
        match s.to_lowercase().as_str() {
            "not null" => Ok(ColumnConstraintSyntax::NotNull),
            "rowtime" => Ok(ColumnConstraintSyntax::Rowtime),
            x => {
                eprintln!("Unexpected constraint parsed: {}", x);
                unreachable!();
            }
        }
    }

    /*
     * ----------------------------------------------------------------------------
     * Column Definitions
     * ----------------------------------------------------------------------------
     */

    fn parse_column_definition(mut params: FnParseParams) -> Result<ColumnDefinition> {
        let column_name = parse_child(
            &mut params,
            Rule::column_name,
            Self::parse_column_name,
            identity,
        )?;
        let data_type = parse_child(
            &mut params,
            Rule::data_type,
            Self::parse_data_type,
            identity,
        )?;
        let column_constraints_syntax = parse_child_seq(
            &mut params,
            Rule::column_constraint,
            &Self::parse_column_constraint,
            &identity,
        )?;

        let not_null = column_constraints_syntax
            .iter()
            .any(|constraint_syntax| matches!(constraint_syntax, ColumnConstraintSyntax::NotNull));
        let column_data_type = ColumnDataType::new(column_name, data_type, !not_null);

        let column_constraints = column_constraints_syntax
            .into_iter()
            .filter_map(|constraint_syntax| match constraint_syntax {
                ColumnConstraintSyntax::Rowtime => Some(ColumnConstraint::Rowtime),
                ColumnConstraintSyntax::NotNull => None,
            })
            .collect::<Vec<_>>();

        Ok(ColumnDefinition::new(column_data_type, column_constraints))
    }

    /*
     * ----------------------------------------------------------------------------
     * Option Specifications
     * ----------------------------------------------------------------------------
     */
    fn parse_option_specifications(mut params: FnParseParams) -> Result<Vec<OptionSyntax>> {
        parse_child_seq(
            &mut params,
            Rule::option_specification,
            &Self::parse_option_specification,
            &identity,
        )
    }

    fn parse_option_specification(mut params: FnParseParams) -> Result<OptionSyntax> {
        let option_name = parse_child(
            &mut params,
            Rule::option_name,
            Self::parse_option_name,
            identity,
        )?;

        let option_value = parse_child(
            &mut params,
            Rule::option_value,
            Self::parse_option_value,
            identity,
        )?;

        Ok(OptionSyntax {
            option_name,
            option_value,
        })
    }
}
