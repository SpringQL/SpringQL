// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::collections::VecDeque;

use anyhow::Context;
use pest::iterators::{Pair, Pairs};

use crate::{
    api::error::{Result, SpringError},
    sql_processor::sql_parser::pest_parser_impl::generated_parser::Rule,
};

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub(super) struct FnParseParams<'a> {
    pub(super) sql: &'a str,

    /// Collected from Pairs.
    ///
    /// Pairs itself cannot be used as this struct field:
    /// An AST node who has multiple children can call parse_self!() / parse_leaf_string!() macro twice or more.
    /// But Pairs::next() takes this field's ownership so it fails in 2nd macro call.
    /// On the other hand, VecDeque::pop_front() just borrows this field and returns ownership of Pair.
    pub(super) children_pairs: VecDeque<Pair<'a, Rule>>,

    /// Used for leaves.
    pub(super) self_string: String,
}

/// Parse the next child term as `child_term` by `child_parser`.
///
/// Returns Ok(None) when either of the following cases:
///  - no child term left.
///  - the next child term does not match $child_term.
///
/// # Failures
///
/// - [SpringError::Sql](crate::error::SpringError::Sql) when:
///   - When no child term left.
///   - When the next child term does not match $child_term.
///   - Raises Err from `child_parser` as-is.
pub(super) fn parse_child<T, ChildRet>(
    params: &mut FnParseParams,
    child_term: Rule,
    child_parser: impl Fn(FnParseParams) -> Result<ChildRet>,
    ret_closure: impl Fn(ChildRet) -> T,
) -> Result<T> {
    let child_pair: Pair<Rule> = params
        .children_pairs
        .pop_front()
        .with_context(|| {
            format!(
                "Tried to parse a term `{:?}` but nothing left: {}\n{:#?}",
                child_term, params.sql, params
            )
        })
        .map_err(SpringError::Sql)?;

    if child_pair.as_rule() == child_term {
        let child_str = child_pair.as_str();
        let grand_children_pairs: Pairs<Rule> = child_pair.into_inner();

        let child_params = FnParseParams {
            sql: params.sql,
            children_pairs: grand_children_pairs.collect(),
            self_string: child_str.to_string(),
        };
        let child_ast = child_parser(child_params)?;

        Ok(ret_closure(child_ast))
    } else {
        panic!(
            "Hit to unexpected rule: got({:?}); expected({:?})\n\
        Pair: {}\n\
        {:#?}
        ",
            child_pair.as_rule(),
            child_term,
            child_pair,
            params
        );
    }
}

/// Try to parse the next child term as `child_term` by `child_parser`.
///
/// Returns Ok(None) when either of the following cases:
/// - no child term left.
/// - the next child term does not match $child_term.
///
/// # Failures
/// Raises Err from `child_parser` as-is.
pub(super) fn try_parse_child<T, ChildRet>(
    params: &mut FnParseParams,
    child_term: Rule,
    child_parser: impl Fn(FnParseParams) -> Result<ChildRet>,
    ret_closure: impl Fn(ChildRet) -> T,
) -> Result<Option<T>> {
    if let Some(child_pair) = params.children_pairs.pop_front() {
        if child_pair.as_rule() == child_term {
            let child_str = child_pair.as_str();
            let grand_children_pairs: Pairs<Rule> = child_pair.into_inner();

            let child_params = FnParseParams {
                sql: params.sql,
                children_pairs: grand_children_pairs.collect(),
                self_string: child_str.to_string(),
            };
            let child_ast = child_parser(child_params)?;

            Ok(Some(ret_closure(child_ast)))
        } else {
            params.children_pairs.push_front(child_pair);
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Parses children sequence by `child_parser` while next child matches `child_term`.
///
/// # Failures
/// Raises Err from `child_parser` as-is.
pub(super) fn parse_child_seq<T, ChildRet>(
    params: &mut FnParseParams,
    child_term: Rule,
    child_parser: &impl Fn(FnParseParams) -> Result<ChildRet>,
    ret_closure: &impl Fn(ChildRet) -> T,
) -> Result<Vec<T>> {
    let mut children = Vec::<T>::new();
    while let Some(child) = try_parse_child(params, child_term, child_parser, ret_closure)? {
        children.push(child);
    }
    Ok(children)
}

pub(super) fn self_as_str<'a>(params: &'a mut FnParseParams) -> &'a str {
    params.self_string.as_str()
}
