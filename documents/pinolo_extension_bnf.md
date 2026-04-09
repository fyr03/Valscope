## Pinolo_extension Implemented Syntax BNF Summary

### Top-Level Statements
```bnf
statement ::= query_spec
            | select_for_update
            | common_table_expression
```

### Queries
```bnf
query_spec ::= "select" [set_quantifier] select_list
               from_clause
               ["where" bool_expr]
               [group_clause]
               [order_by]
               [limit_clause]

set_quantifier ::= "distinct" | ε
limit_clause   ::= "limit" integer
order_by       ::= "order by" order_item { "," order_item }
order_item     ::= value_expr ("asc" | "desc")

select_for_update ::= query_spec ["for" lockmode]
lockmode ::= "update" | "share" | "no key update" | "key share"
```

### Common Table Expressions (WITH)
```bnf
common_table_expression ::= "with" with_item { "," with_item } query_spec ["for" lockmode]
with_item ::= ident "as" "(" query_spec ")"
```

### FROM and Table References
```bnf
from_clause ::= "from" table_ref { "," table_ref }

table_ref ::= table_or_query_name
            | table_subquery
            | joined_table

table_or_query_name ::= table_name "as" ident

table_subquery   ::= "(" query_spec ")" "as" ident

joined_table ::= table_ref join_type "join" table_ref ["on" "(" join_cond ")"]

join_type ::= "inner"
            | "left outer"
            | "right outer"
            | "cross"

join_cond ::= simple_join_cond | expr_join_cond
simple_join_cond ::= qualified_col "=" qualified_col
expr_join_cond ::= bool_expr
```

### GROUP BY and HAVING
```bnf
group_clause   ::= "group by" column_reference [ "," column_reference ] ["having" bool_expr]
```

### SELECT List
```bnf
select_list ::= value_expr ["as" ident] { "," value_expr ["as" ident] }
```

### Boolean Expressions
```bnf
bool_expr ::= comparison_op
            | bool_term
            | null_predicate
            | truth_value
            | exists_predicate

comparison_op ::= value_expr comp_op value_expr
comp_op ::= "=" | "<>" | "<" | ">" | "<=" | ">=" | "like" | "regexp" | "rlike" | "between"

bool_term ::= "(" bool_expr ")" ("and" | "or") "(" bool_expr ")"
null_predicate ::= value_expr "is" ["not"] "null"
truth_value ::= TRUE | FALSE
exists_predicate ::= "exists" "(" query_spec ")"
```

### Value Expressions
```bnf
value_expr ::= function_call
             | atomic_subselect
             | case_expr
             | column_reference
             | const_expr

function_call ::= func_name "(" [ casted_arg { "," casted_arg } ] ")"
casted_arg ::= value_expr | "cast(" value_expr "as" type_name ")"

atomic_subselect ::= "(" query_spec ")"

case_expr ::= "case" "when" bool_expr "then" value_expr "else" value_expr "end"

column_reference ::= ident ["." ident]

const_expr ::= integer_literal
             | string_literal
             | numeric_literal
             | TRUE | FALSE
             | "null"
```

### UNION/INTERSECT/EXCEPT
```bnf
unioned_query ::= query_spec compound_op query_spec
compound_op   ::= "union" | "union all" | "intersect" | "except"
```

## Syntax Features Added Beyond PINOLO_BNF


### 1. Complete JOIN Syntax
```bnf
joined_table ::= table_ref join_type "join" table_ref ["on" "(" join_cond ")"]

join_type ::= "inner"
            | "left outer" 
            | "right outer"
            | "cross"

join_cond ::= simple_join_cond | expr_join_cond
simple_join_cond ::= qualified_col "=" qualified_col
expr_join_cond ::= bool_expr
```
**Note**: PINOLO_BNF only has basic JOIN representation, while the current implementation supports complete JOIN types and condition syntax.

### 2. Common Table Expressions (CTE)
```bnf
common_table_expression ::= "with" with_item { "," with_item } query_spec ["for" lockmode]
with_item ::= ident "as" "(" query_spec ")"
```
**Note**: PINOLO_BNF does not mention WITH clauses at all, while the current implementation supports complete CTE syntax.

### 3. SELECT FOR UPDATE Locking
```bnf
select_for_update ::= query_spec ["for" lockmode]
lockmode ::= "update" | "share" | "no key update" | "key share"
```
**Note**: PINOLO_BNF does not include transaction locking syntax, while the current implementation supports multiple lock modes.

### 4. Complete GROUP BY and HAVING
```bnf
group_clause ::= "group by" column_reference [ "," column_reference ] ["having" bool_expr]
```
**Note**: PINOLO_BNF only mentions basic GROUP BY, while the current implementation supports multi-column grouping and HAVING condition filtering.

### 5. ORDER BY and LIMIT Clauses
```bnf
order_by ::= "order by" order_item { "," order_item }
order_item ::= value_expr ("asc" | "desc")
limit_clause ::= "limit" integer
```
**Note**: PINOLO_BNF lacks syntax definitions for result set sorting and limiting.

### 6. Rich Function Calls
```bnf
function_call ::= func_name "(" [ casted_arg { "," casted_arg } ] ")"
casted_arg ::= value_expr | "cast(" value_expr "as" type_name ")"
```
**Note**: The current implementation supports function parameters with type casting, while PINOLO_BNF only has basic function calls.

### 7. CASE Expressions
```bnf
case_expr ::= "case" "when" bool_expr "then" value_expr "else" value_expr "end"
```
**Note**: PINOLO_BNF lacks conditional expression syntax.

### 8. Table Aliases and Column References
```bnf
table_or_query_name ::= table_name "as" ident
table_subquery ::= "(" query_spec ")" "as" ident  
column_reference ::= ident ["." ident]
```
**Note**: The current implementation supports complete table aliases and qualified column name syntax.

### 9. Rich Comparison Operators
```bnf
comp_op ::= "=" | "<>" | "<" | ">" | "<=" | ">=" | "like" | "regexp" | "rlike" | "between"
```
**Note**: PINOLO_BNF only has basic comparison operators, while the current implementation supports pattern matching and range comparison.

### 10. NULL Predicates
```bnf
null_predicate ::= value_expr "is" ["not"] "null"
```
**Note**: PINOLO_BNF lacks dedicated NULL value checking syntax.

## Summary

The current implementation has mainly added the following features compared to PINOLO_BNF:
- **Complete JOIN operations** - Supporting multiple JOIN types and conditions
- **Advanced query features** - CTE, FOR UPDATE locking, ORDER BY/LIMIT
- **Rich expressions** - CASE expressions, function calls, type casting
- **Complete grouping and aggregation** - Multi-column GROUP BY and HAVING filtering
- **Practical SQL extensions** - Table aliases, pattern matching, NULL checking, etc.

These extensions make the current implementation closer to actual SQL dialects in use, while the diagram BNF is more theoretical and focuses on core syntax.