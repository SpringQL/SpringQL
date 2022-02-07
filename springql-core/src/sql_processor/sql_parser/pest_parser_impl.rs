// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod generated_parser;
mod helper;

use crate::error::{Result, SpringError};
use crate::expression::boolean_expression::comparison_function::ComparisonFunction;
use crate::expression::boolean_expression::numerical_function::NumericalFunction;
use crate::expression::boolean_expression::BooleanExpr;
use crate::expression::function_call::FunctionCall;
use crate::expression::operator::{BinaryOperator, UnaryOperator};
use crate::expression::ValueExpr;
use crate::pipeline::field::field_name::ColumnReference;
use crate::pipeline::name::{
    ColumnName, CorrelationAlias, PumpName, SinkWriterName, SourceReaderName, StreamName,
    ValueAlias,
};
use crate::pipeline::option::options_builder::OptionsBuilder;
use crate::pipeline::relation::column::column_constraint::ColumnConstraint;
use crate::pipeline::relation::column::column_data_type::ColumnDataType;
use crate::pipeline::relation::column::column_definition::ColumnDefinition;
use crate::pipeline::relation::sql_type::SqlType;
use crate::pipeline::sink_writer_model::sink_writer_type::SinkWriterType;
use crate::pipeline::sink_writer_model::SinkWriterModel;
use crate::pipeline::source_reader_model::source_reader_type::SourceReaderType;
use crate::pipeline::source_reader_model::SourceReaderModel;
use crate::pipeline::stream_model::stream_shape::StreamShape;
use crate::pipeline::stream_model::StreamModel;
use crate::sql_processor::sql_parser::syntax::{
    ColumnConstraintSyntax, OptionSyntax, SelectStreamSyntax,
};
use crate::stream_engine::command::insert_plan::InsertPlan;
use crate::stream_engine::{NnSqlValue, SqlValue};
use anyhow::{anyhow, Context};
use generated_parser::{GeneratedParser, Rule};
use helper::{parse_child, parse_child_seq, self_as_str, try_parse_child, FnParseParams};
use pest::{iterators::Pairs, Parser};
use std::convert::identity;

use super::parse_success::ParseSuccess;
use super::syntax::{FromItemSyntax, SelectFieldSyntax};

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

    fn parse_constant(mut params: FnParseParams) -> Result<SqlValue> {
        try_parse_child(
            &mut params,
            Rule::null_constant,
            |_| Ok(SqlValue::Null),
            identity,
        )?
        .or(try_parse_child(
            &mut params,
            Rule::numeric_constant,
            Self::parse_numeric_constant,
            identity,
        )?)
        .or(try_parse_child(
            &mut params,
            Rule::string_constant,
            Self::parse_string_constant,
            identity,
        )?)
        .ok_or_else(|| SpringError::Sql(anyhow!("Does not match any child rule of constant.",)))
    }

    fn parse_numeric_constant(mut params: FnParseParams) -> Result<SqlValue> {
        parse_child(
            &mut params,
            Rule::integer_constant,
            Self::parse_integer_constant,
            identity,
        )
    }

    fn parse_integer_constant(mut params: FnParseParams) -> Result<SqlValue> {
        let s = self_as_str(&mut params);

        s.parse::<i16>()
            .map(|i| SqlValue::NotNull(NnSqlValue::SmallInt(i)))
            .or_else(|_| {
                s.parse::<i32>()
                    .map(|i| SqlValue::NotNull(NnSqlValue::Integer(i)))
            })
            .or_else(|_| {
                s.parse::<i64>()
                    .map(|i| SqlValue::NotNull(NnSqlValue::BigInt(i)))
            })
            .map_err(|_e| {
                SpringError::Sql(anyhow!(
                    "integer value `{}` could not be parsed as i64 (max supported size)",
                    s
                ))
            })
    }

    fn parse_string_constant(mut params: FnParseParams) -> Result<SqlValue> {
        parse_child(
            &mut params,
            Rule::string_content,
            Self::parse_string_content,
            |s| SqlValue::NotNull(NnSqlValue::Text(s)),
        )
    }

    fn parse_string_content(mut params: FnParseParams) -> Result<String> {
        let s = self_as_str(&mut params);
        Ok(s.into())
    }

    /*
     * ----------------------------------------------------------------------------
     * Operators
     * ----------------------------------------------------------------------------
     */

    fn parse_unary_operator(mut params: FnParseParams) -> Result<UnaryOperator> {
        let s = self_as_str(&mut params);
        match s {
            "-" => Ok(UnaryOperator::Minus),
            _ => Err(SpringError::Sql(anyhow!(
                "Does not match any child rule of unary_operator.",
            ))),
        }
    }

    fn parse_binary_operator(mut params: FnParseParams) -> Result<BinaryOperator> {
        let s = self_as_str(&mut params);
        match s.to_lowercase().as_str() {
            "=" => Ok(BinaryOperator::Equal),
            "+" => Ok(BinaryOperator::Add),
            _ => Err(SpringError::Sql(anyhow!(
                "Does not match any child rule of binary_operator.",
            ))),
        }
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
            Rule::create_source_reader_command,
            Self::parse_create_source_reader_command,
            identity,
        )?)
        .or(try_parse_child(
            &mut params,
            Rule::create_sink_stream_command,
            Self::parse_create_sink_stream_command,
            identity,
        )?)
        .or(try_parse_child(
            &mut params,
            Rule::create_sink_writer_command,
            Self::parse_create_sink_writer_command,
            identity,
        )?)
        .or(try_parse_child(
            &mut params,
            Rule::create_pump_command,
            Self::parse_create_pump_command,
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
        let source_stream_name = parse_child(
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

        let stream_shape = StreamShape::new(column_definitions)?;
        let source_stream = StreamModel::new(source_stream_name, stream_shape);

        Ok(ParseSuccess::CreateSourceStream(source_stream))
    }

    /*
     * ----------------------------------------------------------------------------
     * CREATE SOURCE READER
     * ----------------------------------------------------------------------------
     */

    fn parse_create_source_reader_command(mut params: FnParseParams) -> Result<ParseSuccess> {
        let source_reader_name = parse_child(
            &mut params,
            Rule::source_reader_name,
            Self::parse_source_reader_name,
            identity,
        )?;
        let source_stream_name = parse_child(
            &mut params,
            Rule::stream_name,
            Self::parse_stream_name,
            identity,
        )?;
        let source_reader_type = parse_child(
            &mut params,
            Rule::source_reader_type,
            Self::parse_source_reader_type,
            identity,
        )?;
        let option_syntaxes = try_parse_child(
            &mut params,
            Rule::option_specifications,
            &Self::parse_option_specifications,
            &identity,
        )?;

        let mut options = OptionsBuilder::default();
        if let Some(option_syntaxes) = option_syntaxes {
            for o in option_syntaxes {
                options = options.add(o.option_name, o.option_value);
            }
        }
        let options = options.build();

        let source_reader = SourceReaderModel::new(
            source_reader_name,
            source_reader_type,
            source_stream_name,
            options,
        );

        Ok(ParseSuccess::CreateSourceReader(source_reader))
    }

    /*
     * ----------------------------------------------------------------------------
     * CREATE SINK STREAM
     * ----------------------------------------------------------------------------
     */

    fn parse_create_sink_stream_command(mut params: FnParseParams) -> Result<ParseSuccess> {
        let sink_stream_name = parse_child(
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

        let stream_shape = StreamShape::new(column_definitions)?;
        let sink_stream = StreamModel::new(sink_stream_name, stream_shape);

        Ok(ParseSuccess::CreateSinkStream(sink_stream))
    }

    /*
     * ----------------------------------------------------------------------------
     * CREATE SINK WRITER
     * ----------------------------------------------------------------------------
     */

    fn parse_create_sink_writer_command(mut params: FnParseParams) -> Result<ParseSuccess> {
        let sink_writer_name = parse_child(
            &mut params,
            Rule::sink_writer_name,
            Self::parse_sink_writer_name,
            identity,
        )?;
        let sink_stream_name = parse_child(
            &mut params,
            Rule::stream_name,
            Self::parse_stream_name,
            identity,
        )?;
        let sink_writer_type = parse_child(
            &mut params,
            Rule::sink_writer_type,
            Self::parse_sink_writer_type,
            identity,
        )?;
        let option_syntaxes = try_parse_child(
            &mut params,
            Rule::option_specifications,
            &Self::parse_option_specifications,
            &identity,
        )?;

        let mut options = OptionsBuilder::default();
        if let Some(option_syntaxes) = option_syntaxes {
            for o in option_syntaxes {
                options = options.add(o.option_name, o.option_value);
            }
        }
        let options = options.build();

        let sink_writer = SinkWriterModel::new(
            sink_writer_name,
            sink_writer_type,
            sink_stream_name,
            options,
        );

        Ok(ParseSuccess::CreateSinkWriter(sink_writer))
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
     * SELECT
     * ----------------------------------------------------------------------------
     */

    fn parse_select_stream(mut params: FnParseParams) -> Result<SelectStreamSyntax> {
        let fields = parse_child_seq(
            &mut params,
            Rule::select_field,
            &Self::parse_select_field,
            &identity,
        )?;
        let from_item = parse_child(
            &mut params,
            Rule::from_item,
            Self::parse_from_item,
            identity,
        )?;

        Ok(SelectStreamSyntax { fields, from_item })
    }

    fn parse_select_field(mut params: FnParseParams) -> Result<SelectFieldSyntax> {
        let value_expr = parse_child(
            &mut params,
            Rule::value_expr,
            Self::parse_value_expr,
            identity,
        )?;
        let alias = try_parse_child(
            &mut params,
            Rule::value_alias,
            Self::parse_value_alias,
            identity,
        )?;
        Ok(SelectFieldSyntax { value_expr, alias })
    }

    fn parse_from_item(mut params: FnParseParams) -> Result<FromItemSyntax> {
        let from_item = parse_child(
            &mut params,
            Rule::sub_from_item,
            Self::parse_sub_from_item,
            identity,
        )?;
        Ok(from_item)
    }

    fn parse_sub_from_item(mut params: FnParseParams) -> Result<FromItemSyntax> {
        let stream_name = parse_child(
            &mut params,
            Rule::stream_name,
            Self::parse_stream_name,
            identity,
        )?;
        let alias = try_parse_child(
            &mut params,
            Rule::correlation_alias,
            Self::parse_correlation_alias,
            identity,
        )?;
        Ok(FromItemSyntax::StreamVariant { stream_name, alias })
    }

    /*
     * ================================================================================================
     * Value Expressions:
     * ================================================================================================
     */

    fn parse_value_expr(mut params: FnParseParams) -> Result<ValueExpr> {
        let expr = parse_child(
            &mut params,
            Rule::sub_value_expr,
            Self::parse_sub_value_expr,
            identity,
        )?;

        if let Some(bin_op) = try_parse_child(
            &mut params,
            Rule::binary_operator,
            Self::parse_binary_operator,
            identity,
        )? {
            let right_expr = parse_child(
                &mut params,
                Rule::value_expr,
                Self::parse_value_expr,
                identity,
            )?;

            match bin_op {
                BinaryOperator::Equal => Ok(ValueExpr::BooleanExpr(
                    BooleanExpr::ComparisonFunctionVariant(ComparisonFunction::EqualVariant {
                        left: Box::new(expr),
                        right: Box::new(right_expr),
                    }),
                )),
                BinaryOperator::Add => Ok(ValueExpr::BooleanExpr(
                    BooleanExpr::NumericalFunctionVariant(NumericalFunction::AddVariant {
                        left: Box::new(expr),
                        right: Box::new(right_expr),
                    }),
                )),
            }
        } else {
            Ok(expr)
        }
    }

    fn parse_sub_value_expr(mut params: FnParseParams) -> Result<ValueExpr> {
        try_parse_child(
            &mut params,
            Rule::constant,
            Self::parse_constant,
            ValueExpr::Constant,
        )?
        .or(try_parse_child(
            &mut params,
            Rule::column_reference,
            Self::parse_column_reference,
            ValueExpr::ColumnReference,
        )?)
        .or({
            if let Some(uni_op) = try_parse_child(
                &mut params,
                Rule::unary_operator,
                Self::parse_unary_operator,
                identity,
            )? {
                Some(parse_child(
                    &mut params,
                    Rule::value_expr,
                    Self::parse_value_expr,
                    |expr| ValueExpr::UnaryOperator(uni_op.clone(), Box::new(expr)),
                )?)
            } else {
                None
            }
        })
        .or(try_parse_child(
            &mut params,
            Rule::function_call,
            Self::parse_function_call,
            ValueExpr::FunctionCall,
        )?)
        .ok_or_else(|| {
            SpringError::Sql(anyhow!("Does not match any child rule of sub_value_expr.",))
        })
    }

    /*
     * ----------------------------------------------------------------------------
     * Column Reference
     * ----------------------------------------------------------------------------
     */

    fn parse_column_reference(mut params: FnParseParams) -> Result<ColumnReference> {
        let correlation = parse_child(
            &mut params,
            Rule::correlation,
            Self::parse_correlation,
            identity,
        )?;
        let column_name = parse_child(
            &mut params,
            Rule::column_name,
            Self::parse_column_name,
            identity,
        )?;
        Ok(ColumnReference::new(correlation, column_name))
    }

    /*
     * ----------------------------------------------------------------------------
     * Function
     * ----------------------------------------------------------------------------
     */

    fn parse_function_call(mut params: FnParseParams) -> Result<FunctionCall<ValueExpr>> {
        let function_name = parse_child(
            &mut params,
            Rule::function_name,
            Self::parse_function_name,
            identity,
        )?;
        let parameters = parse_child_seq(
            &mut params,
            Rule::value_expr,
            &Self::parse_value_expr,
            &identity,
        )?;

        match function_name.to_lowercase().as_str() {
            "duration_secs" => {
                if parameters.len() == 1 {
                    Ok(FunctionCall::DurationSecs {
                        duration_secs: Box::new(parameters[0].clone()),
                    })
                } else {
                    Err(SpringError::Sql(anyhow!(
                        "duration_secs() takes exactly one parameter (duration_secs)."
                    )))
                }
            }
            "floor_time" => {
                if parameters.len() == 2 {
                    Ok(FunctionCall::FloorTime {
                        target: Box::new(parameters[0].clone()),
                        resolution: Box::new(parameters[1].clone()),
                    })
                } else {
                    Err(SpringError::Sql(anyhow!(
                        "floor_time() takes exactly two parameters (target, resolution)."
                    )))
                }
            }
            _ => Err(SpringError::Sql(anyhow!(
                "unknown function {}",
                function_name.to_lowercase()
            ))),
        }
    }

    fn parse_function_name(mut params: FnParseParams) -> Result<String> {
        Ok(self_as_str(&mut params).to_string())
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
    fn parse_source_reader_type(mut params: FnParseParams) -> Result<SourceReaderType> {
        let typ = parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            identity,
        )?;
        match typ.as_ref() {
            "NET_SERVER" => Ok(SourceReaderType::Net),
            _ => Err(SpringError::Sql(anyhow!(
                "Invalid source reader name: {}",
                typ
            ))),
        }
    }

    fn parse_sink_writer_name(mut params: FnParseParams) -> Result<SinkWriterName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            SinkWriterName::new,
        )
    }
    fn parse_sink_writer_type(mut params: FnParseParams) -> Result<SinkWriterType> {
        let typ = parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            identity,
        )?;
        match typ.as_ref() {
            "NET_SERVER" => Ok(SinkWriterType::Net),
            "IN_MEMORY_QUEUE" => Ok(SinkWriterType::InMemoryQueue),
            _ => Err(SpringError::Sql(anyhow!(
                "Invalid source reader name: {}",
                typ
            ))),
        }
    }

    fn parse_column_name(mut params: FnParseParams) -> Result<ColumnName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            ColumnName::new,
        )
    }

    fn parse_value_alias(mut params: FnParseParams) -> Result<ValueAlias> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            ValueAlias::new,
        )
    }

    fn parse_correlation(mut params: FnParseParams) -> Result<StreamName> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            StreamName::new,
        )
    }

    fn parse_correlation_alias(mut params: FnParseParams) -> Result<CorrelationAlias> {
        parse_child(
            &mut params,
            Rule::identifier,
            Self::parse_identifier,
            CorrelationAlias::new,
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
            Rule::string_content,
            Self::parse_string_content,
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
