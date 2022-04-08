// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

/// Column with data type.
#[derive(Clone, Eq, PartialEq, Hash, Debug, new)]
pub(crate) enum ColumnConstraint {
    Rowtime,
}
