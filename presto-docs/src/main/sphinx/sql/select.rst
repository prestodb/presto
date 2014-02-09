======
SELECT
======

Synopsis
--------

.. code-block:: none

    [ WITH with_query [, ...] ]
    SELECT [ ALL | DISTINCT ] select_expr [, ...]
    [ FROM from_item [, ...] ]
    [ WHERE condition ]
    [ GROUP BY expression [, ...] ]
    [ HAVING condition]
    [ UNION [ ALL | DISTINCT ] select ]
    [ ORDER BY expression [ ASC | DESC ] [, ...] ]
    [ LIMIT count ]

where ``from_item`` is one of

.. code-block:: none

    table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]

.. code-block:: none

    from_item join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]

Description
-----------

Retrieve rows from zero or more tables.

TABLESAMPLE
-----------

Sample method can be specified as ``BERNOULLI`` or ``SYSTEM``.

Bernoulli method
	Each row is selected to be in the table sample with a probability of samplePercentage/100. When a table is sampled using the Bernoulli method, all physical blocks of the table are scanned and certain rows are skipped (based on the comparison between the samplePercentage and random value calculated at runtime). The probability of a row being included in the result is independent from any other row. This does not reduce the time required to read the sampled table from disk. It may have an impact on the total query time if the sampled output is processed further.

System method
	This sampling method divides the table into logical segments of data and samples the table at this granularity. This sampling method either selects all the rows from a particular segment of data or skips it (based on the comparison between the samplePercentage and random value calculated at runtime). The rows selected in a system sampling will be dependent on which connector is used. For example, when used with Hive, it is dependent on how the data is laid out on HDFS. This method does not guarantee independent sampling probabilities.

.. note:: Neither of the two methods allow deterministic bounds on the number of rows returned.

Example::

	SELECT *
	FROM dim_all_users
	TABLESAMPLE BERNOULLI (50);

	SELECT *
	FROM dim_all_users
	TABLESAMPLE SYSTEM (75);

Using with joins::

	SELECT t1.*, t2.*
	FROM
	table1 t1 TABLESAMPLE SYSTEM (10)
	JOIN
	table2 t2 TABLESAMPLE BERNOULLI (40)
	ON
	t1.id = t2.id

UNION Clause
------------

The UNION clause is used to combine the results of more than one
select statement into a single result set.  The argument to a UNION
clause is another select statement.

.. code-block:: none

    select_statement UNION [ALL | DISTINCT] select_statement

The argument ALL or DISTINCT controls which results are included in
the final result set. If the argument ALL is specified all results are
included even if the results are identical.  If the argument DISTINCT
is specified only distinct results are included in the combined result
set. If neither ALL nor DISTINCT is specified the behavior of the
UNION clause defaults to the behavior specified by DISTINCT.

The following is an example of one of the simplest possible UNION
clauses. The following query selects the bigint value 1 and combines
this result set with a second select statement which selects the
bigint value 2.

.. code-block:: none

    presto:default> select 1 union select 2;
     _col0 
    -------
         2 
         1 
    (2 rows)

To illustrate the behavior of ALL of DISTINCT, consider the following
query example:

.. code-block:: none

    presto:default> select 1 union select 1;
     _col0 
    -------
         1 
    (1 row)

The query shown above doesn't specific ALL or DISTINCT, so the UNION
clause defaults to DISTINCT behavior. The query shown above is
equivalent to ``select 1 union distinct select 1;``.

Next consider the output of the same query with a UNION clause that
specifies ALL behavior:

.. code-block:: none

    presto:default> select 1 union all select 1;
     _col0 
    -------
         1 
         1 
    (2 rows)

Note that Presto will make no attempt to make result sets with
incompatible types compatible.  The following query will produce an
error as the query is attempting to union two select statements with
different column types.

.. code-block:: none

    presto:default> select CAST(1 as varchar) union select 2;
    
    Query 20140209_174939_00046_qhay4 failed: Union query terms have
    mismatched columns

More than two select statements can be combined with multiple union
statments. The type of union, either ALL or DISTINCT, of the first
union influences the type of union for subsequent union
statements. For example, the following statement produces a union of
three select statements with distinct elements in the final result
set:

.. code-block:: none

    presto:default> select 1 union \
                    select 1 union \
                    select 1;
     _col0 
    -------
         1 
    (1 row)

If an ALL is specified on the first UNION clause, the result set will
include all results from three select statments:

.. code-block:: none

    presto:default> select 1 union all \
                    select 1 union \
                    select 1;
     _col0 
    -------
         1 
         1 
         1 
    (3 rows)

To clarify the behavior of ALL or DISTINCT when using multiple UNION
clauses, note the behavior of the following statement with two UNION
clauses. The first clause specifies ALL and the second UNION clause
specifies DISTINCT. In this case the result of two UNION clauses uses
the behavior specified by the first UNION clause which is ALL.

.. code-block:: none

    presto:default> select 1 union all \
                    select 1 union distinct \
                    select 1;
     _col0 
    -------
         1 
         1 
         1 
    (3 rows)

ORDER BY Clause
---------------

The ORDER BY clause is used to sort a result set of a select statement
by one or more columns. This clause has the following structure:

.. code-block:: none

    ORDER BY expression [ ASC | DESC ] [, ...]

Expression can be a column name or a function call which produces a
numeric, character, or boolean value to be sorted.  ORDER BY clauses
can contain one or more expressions to be evaluated for each row of a
result set.

Consider the following example which sorts the union of three select
statements.

.. code-block:: none

    presto:default> select 2 as value union \
                    select 1 as value union \
                    select 4 as value \
                          order by value asc;
     value 
    -------
         1 
         2 
         4 
    (3 rows)

An ORDER BY clause can also contain an expression that evaluates a
function against a column value.  Consider the output of the following
statement which sorts numeric values by absolute value.

.. code-block:: none

    presto:default> select -12 as value union \
                    select 2 as value union \
                    select -1 as value \
                        order by abs(value) asc;
     value 
    -------
        -1 
         2 
       -12 
    (3 rows)

LIMIT Clause
------------

The LIMIT clause has the following syntax:

.. code-block:: none

    LIMIT count

Specifying a LIMIT count value restricts the query output to a limited
number of records. The following example queries a table with 7.5
million rows, but the limit clause limits the output to only five
rows:

.. code-block:: none

    presto:default> select o_orderdate from orders limit 5;
     o_orderdate 
    -------------
     1996-04-14  
     1992-01-15  
     1995-02-01  
     1995-11-12  
     1992-04-26  
    (5 rows)