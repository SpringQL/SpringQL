mod generated_parser;
mod helper;

use crate::error::{Result, SpringError};
use crate::pipeline::foreign_stream_model::ForeignStreamModel;
use crate::pipeline::name::{ColumnName, ServerName, StreamName};
use crate::pipeline::option::options_builder::OptionsBuilder;
use crate::pipeline::relation::column::column_constraint::ColumnConstraint;
use crate::pipeline::relation::column::column_data_type::ColumnDataType;
use crate::pipeline::relation::column::column_definition::ColumnDefinition;
use crate::pipeline::relation::sql_type::SqlType;
use crate::pipeline::server_model::server_type::ServerType;
use crate::pipeline::server_model::ServerModel;
use crate::pipeline::stream_model::stream_shape::StreamShape;
use crate::pipeline::stream_model::StreamModel;
use crate::stream_engine::command::alter_pipeline_command::AlterPipelineCommand;
use crate::stream_engine::command::Command;
use anyhow::{anyhow, Context};
use generated_parser::{GeneratedParser, Rule};
use helper::{parse_child, parse_child_seq, self_as_str, try_parse_child, FnParseParams};
use pest::{iterators::Pairs, Parser};
use std::convert::identity;
use std::sync::Arc;

use self::helper::{ColumnConstraintSyntax, OptionSyntax};

#[derive(Debug, Default)]
pub(in crate::sql_parser) struct PestParserImpl;

impl PestParserImpl {
    pub(crate) fn parse<S: Into<String>>(&self, sql: S) -> Result<Command> {
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

    fn parse_command(mut params: FnParseParams) -> Result<Command> {
        try_parse_child(
            &mut params,
            Rule::create_source_stream_command,
            Self::parse_create_source_stream_command,
            Command::AlterPipeline,
        )?
        .or(try_parse_child(
            &mut params,
            Rule::create_sink_stream_command,
            Self::parse_create_sink_stream_command,
            Command::AlterPipeline,
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

    fn parse_create_source_stream_command(
        mut params: FnParseParams,
    ) -> Result<AlterPipelineCommand> {
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
        let server_name = parse_child(
            &mut params,
            Rule::server_name,
            Self::parse_server_name,
            identity,
        )?;
        let option_syntaxes = parse_child(
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
        for o in option_syntaxes {
            options = options.add(o.option_name, o.option_value);
        }
        let options = options.build();

        let server_type = match server_name.as_ref() {
            "NET_SERVER" => Ok(ServerType::SourceNet),
            _ => Err(SpringError::Sql(anyhow!(
                "Invalid server name: {}",
                server_name
            ))),
        }?;
        let server = ServerModel::new(server_type, Arc::new(foreign_stream), options);

        Ok(AlterPipelineCommand::CreateForeignStream(server))
    }

    /*
     * ----------------------------------------------------------------------------
     * CREATE SINK STREAM
     * ----------------------------------------------------------------------------
     */

    fn parse_create_sink_stream_command(mut params: FnParseParams) -> Result<AlterPipelineCommand> {
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
        let server_name = parse_child(
            &mut params,
            Rule::server_name,
            Self::parse_server_name,
            identity,
        )?;
        let option_syntaxes = parse_child(
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
        for o in option_syntaxes {
            options = options.add(o.option_name, o.option_value);
        }
        let options = options.build();

        let server_type = match server_name.as_ref() {
            "NET_SERVER" => Ok(ServerType::SinkNet),
            _ => Err(SpringError::Sql(anyhow!(
                "Invalid server name: {}",
                server_name
            ))),
        }?;
        let server = ServerModel::new(server_type, Arc::new(foreign_stream), options);

        Ok(AlterPipelineCommand::CreateForeignStream(server))
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

    fn parse_server_name(mut params: FnParseParams) -> Result<ServerName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            ServerName::new,
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
