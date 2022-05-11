# module expression

 Expression has two forms:

 1. Value expression, which is evaluated into an SqlValue from a row.
 2. Aggregate expression, which is evaluated into an SqlValue from set of rows.

 Since SQL parser cannot distinguish column reference and value expression,
 `ValueExprOrAlias` is used for value expressions excluding select_list.
