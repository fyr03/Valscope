## Mutation Operator Classification

Based on the classification standards in Mutation Classification.md, this classification is organized into two main categories: set-related and value-related:

### Set-Related

#### Predicate-Based Mutators

**1.1 Comparison Relaxation/Strengthening**

- **FixMCmpOp: Modify Comparison Operators**
  - Expand results: `>` → `>=`, `<` → `<=`, `=` → `>=`
  - Narrow results: `>=` → `>`, `<=` → `<`

**1.2 String-pattern Relaxation**

- **RdMLike: Modify LIKE Expression Patterns**
  - Expand results: Replace regular characters with wildcards, or replace `'_'` with `'%'`
  - Narrow results: Replace `'%'` with `'_'` or fixed characters

- **RdMRegExp: Modify REGEXP Expression Patterns**
  - Expand results: Remove `'^'` and `'$'`, replace `'+'` and `'?'` with `'*'`
  - Narrow results: Add `'^'` and `'$'`, replace `'*'` with `'+'` or `'?'`

#### Relation-Level / Structural Mutators

**2.1 JOIN Condition Mutators**

- **FixMOn: Modify JOIN ON Conditions**
  - Expand results: Replace the original ON condition with `ON 1=1`
  - Narrow results: Replace the original ON condition with `ON 1=0`

**2.2 DISTINCT Mutators**

- **FixMDistinct: Control DISTINCT Keyword in SQL Queries**
  - Add or remove DISTINCT, affecting the deduplication behavior of the result set
  - `SELECT a` → `SELECT DISTINCT a` (under-approx)
  - `SELECT DISTINCT a` → `SELECT a` (over-approx)

**2.3 Set Operators**

- **FixMUnionAll: Toggle Between UNION and UNION ALL**
  - `UNION ALL` → `UNION` (under-approx, removes duplicates)
  - `UNION` → `UNION ALL` (over-approx, preserves duplicates)

**2.4 Logical Structure Mutators**

- **FixMWhere: Modify WHERE Conditions**
  - Expand results: Replace the original WHERE condition with `WHERE 1=1`
  - Narrow results: Replace the original WHERE condition with `WHERE 1=0`

- **FixMHaving: Modify HAVING Conditions**
  - Expand results: Replace the original HAVING condition with `HAVING 1=1`
  - Narrow results: Replace the original HAVING condition with `HAVING 1=0`

- **FixMLogicalAnd: Logical AND Expression Mutation**
  - Mutation pattern: `a AND b AND c` → `a AND b` (removes the last condition)
  - Mutation direction: direction = 1 (expands results: fewer conditions yield more results)

**2.5 JOIN Type Mutators**

- **FixMInnerJoin: JOIN Type Mutation**
  - Mutation pattern: `INNER JOIN` → `LEFT JOIN`
  - Mutation direction: direction = 1 (expands results: LEFT JOIN yields more results)

**2.6 Set Operation Mutators**

- **FixMIntersect: Set Operation Mutation**
  - Mutation pattern: `INTERSECT` → `UNION`
  - Mutation direction: direction = 1 (expands results: INTERSECT -> UNION yields more results)

- **FixMExcept: Set Operation Mutation**
  - Mutation pattern: `A EXCEPT B` → `A` (removes EXCEPT operation, keeps only the left expression)
  - Mutation direction: direction = 1 (expands results: removing EXCEPT operation yields more results)

**2.7 Newly Implemented Set Operation Mutators**

- **FixMLogicalAnd: Logical AND Expression Mutation**
  - Mutation pattern: `condition1 AND condition2` → `condition1` (removes the second condition)
  - Mutation direction: direction = 1 (expands results: fewer conditions yield more results)
  - Implementation status: ✅ Fixed and integrated
  - Description: Removes partial conditions by modifying the `expression` property of the AND expression

- **FixMInnerJoin: INNER JOIN Type Mutation**
  - Mutation pattern: `INNER JOIN` → `LEFT JOIN`
  - Mutation direction: direction = 1 (expands results: LEFT JOIN yields more results)
  - Implementation status: ✅ Fixed and integrated
  - Description: Modifies the `kind` property of the Join node from 'INNER' to 'LEFT'

### Value-Related

#### Aggregation & Grouping Mutators

**3.1 Aggregation Replacement**

- **AggM_MaxToMin: Mutate MAX Function to MIN Function**
  - Mutation pattern: `MAX(c)` → `MIN(c)`
  - Mutation direction: direction = 0 (results become smaller)

- **AggM_MinToMax: Mutate MIN Function to MAX Function**
  - Mutation pattern: `MIN(c)` → `MAX(c)`
  - Mutation direction: direction = 1 (results become larger)

- **AggM_AvgToMin: Mutate AVG Function to MIN Function**
  - Mutation pattern: `AVG(c)` → `MIN(c)`
  - Mutation direction: direction = 0 (results become smaller)

- **AggM_AvgToMax: Mutate AVG Function to MAX Function**
  - Mutation pattern: `AVG(c)` → `MAX(c)`
  - Mutation direction: direction = 1 (results become larger)

- **AggM_AvgAddConstant: Mutate AVG Function to AVG Function Plus Constant**
  - Mutation pattern: `AVG(c)` → `AVG(c) + 2`
  - Mutation direction: direction = 1 (results become larger)
  - Classification: Category 4 - Expression-Level Mutators (Value-Semantic)
  - Description: Performs addition operations on aggregate functions, belonging to expression-level mutations at the value-semantic level

- **AggM_StddevSampToVarSamp: Mutate Sample Standard Deviation to Sample Variance**
  - Mutation pattern: `STDDEV_SAMP(c)` → `VAR_SAMP(c)`
  - Mutation direction: direction = 1 (results become larger when standard deviation is greater than 1)

**3.2 DISTINCT in Aggregation**

- **AggM_CountDistinctToCount: Remove DISTINCT from COUNT**
  - Mutation pattern: `COUNT(DISTINCT c)` → `COUNT(c)`
  - Mutation direction: direction = 1 (results become larger or remain the same)

- **AggM_CountToCountDistinct: Add DISTINCT to COUNT**
  - Mutation pattern: `COUNT(c)` → `COUNT(DISTINCT c)`
  - Mutation direction: direction = 0 (results become smaller or remain the same)

**3.3 Monotonicity-Preserving Mutations**

- **AggM_MonotonicMultiplication: Perform Multiplication on Function Arguments While Preserving Function Monotonicity**
  - Mutation pattern: `FUNC(c)` → `FUNC(c * k)`, where k is a constant and FUNC is a monotonic-preserving aggregate function
  - Mutation direction: Determined by the value of k and function type (preserves monotonicity when k>0)
  - Supported functions: SUM (including SUM(DISTINCT)), AVG, MAX, MIN, etc.
  - Description: For numeric columns, performs multiplication mutations on aggregate function arguments while preserving the monotonicity properties of the function

#### Expression-Level Mutators

**4.1 General Expression Mutators**

- **ExprM_AddConstant: Add Constant to Expression**
  - Mutation pattern: `expr` → `expr + k`, where k is a constant, expr can be:
    - Regular column: `c` → `c + 2`
    - Aggregate function: `AVG(c)` → `AVG(c) + 2`
    - Arithmetic expression: `a + b` → `a + b + 3`
    - Function call: `FUNC(x)` → `FUNC(x) + 1`
  - Mutation direction: direction = 1 (results become larger when k is positive)
  - Classification: Category 4 - Expression-Level Mutators (Value-Semantic)
  - Description: Performs addition operations on any numeric type expression, belonging to expression-level mutations at the value-semantic level

- **ExprM_MultiplyConstant: Multiply Expression by Constant**
  - Mutation pattern: `expr` → `expr * k`, where k is a constant (usually positive to preserve monotonicity), expr can be:
    - Regular column: `c` → `c * 2`
    - Aggregate function: `SUM(c)` → `SUM(c) * 3`
    - Arithmetic expression: `a * b` → `a * b * 0.5`
  - Mutation direction: Determined by the value of k (results become larger when k>1, smaller when 0<k<1)
  - Special handling: For trigonometric functions, only perform addition mutations to avoid unreasonable changes to trigonometric function properties caused by multiplication



