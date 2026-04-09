# Mutation Operator Statistics
Total number of operators: 26
Transforming C1 into C2 achieves the over-approximation; the inverse achieves the under-approximation.

## Mutation Operator Classification

| Type |  C1 | C2 |
|------|----|----|
| **Set-Semantic Mutators**| |  |
|  **Relation** | SELECT DISTINCT a FROM r<br>r1 UNION r2<br> A EXCEPT B<br>A INTERSECT B| SELECT a FROM r<br>r1 UNION ALL r2<br> A <br>A UNION B|
|  **Predicate** | WHERE cond<br>WHERE FALSE<br>col LIKE '_abc%'<br>a < b<br>a = b<br>a > b<br>a AND b AND c<br>ON cond<br>ON 1=0<br>REGEXP '^abc+$'<br>HAVING cond<br>HAVING FALSE<br>INNER JOIN | WHERE TRUE<br>WHERE cond<br>col LIKE '%abc%'<br>a <= b<br>a >= b<br>a >= b<br>a AND b<br>ON TRUE<br>ON cond<br>REGEXP 'abc*'<br>HAVING TRUE<br>HAVING cond<br>LEFT JOIN |
| **Value-Semantic Mutators** | | |
|  **Aggregation** | COUNT(DISTINCT c)<br>AVG(c)<br>MIN(c)<br>VAR_SAMP(c)<br>STDDEV_SAMP(c)<br>SUM(c) | COUNT(c)<br>MAX(c)<br>MAX(c)<br>STDDEV_SAMP(c) (c<1)<br>VAR_SAMP(c) (c>1)<br>SUM(c*k)(k has the same sign as c) |
|  **Expression** |  expr<br>expr<br>FUNC(c) (FUNC is monotonic) |expr + k (k>0)<br>expr * k (k has same sign as expr)<br>FUNC(c * k) (k > 0) |
