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
    [ GROUP BY [ ALL | DISTINCT ] grouping_element [, ...] ]
    [ HAVING condition]
    [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
    [ ORDER BY expression [ ASC | DESC ] [, ...] ]
    [ LIMIT [ count | ALL ] ]

where ``from_item`` is one of

.. code-block:: none

    table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]

.. code-block:: none

    from_item join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]

and ``join_type`` is one of

.. code-block:: none

    [ INNER ] JOIN
    LEFT [ OUTER ] JOIN
    RIGHT [ OUTER ] JOIN
    FULL [ OUTER ] JOIN
    CROSS JOIN

and ``grouping_element`` is one of

.. code-block:: none

    ()
    expression
    GROUPING SETS ( ( column [, ...] ) [, ...] )
    CUBE ( column [, ...] )
    ROLLUP ( column [, ...] )

Description
-----------

Retrieve rows from zero or more tables.

WITH Clause
-----------

The ``WITH`` clause defines named relations for use within a query.
It allows flattening nested queries or simplifying subqueries.
For example, the following queries are equivalent::

    SELECT a, b
    FROM (
      SELECT a, MAX(b) AS b FROM t GROUP BY a
    ) AS x;

    WITH x AS (SELECT a, MAX(b) AS b FROM t GROUP BY a)
    SELECT a, b FROM x;

This also works with multiple subqueries::

    WITH
      t1 AS (SELECT a, MAX(b) AS b FROM x GROUP BY a),
      t2 AS (SELECT a, AVG(d) AS d FROM y GROUP BY a)
    SELECT t1.*, t2.*
    FROM t1
    JOIN t2 ON t1.a = t2.a;

Additionally, the relations within a ``WITH`` clause can chain::

    WITH
      x AS (SELECT a FROM t),
      y AS (SELECT a AS b FROM x),
      z AS (SELECT b AS c FROM y)
    SELECT c FROM z;

GROUP BY Clause
---------------

The ``GROUP BY`` clause divides the output of a ``SELECT`` statement into
groups of rows containing matching values. A simple ``GROUP BY`` clause may
contain any expression composed of input columns or it may be an ordinal
number selecting an output column by position (starting at one).

The following queries are equivalent. They both group the output by
the ``nationkey`` input column with the first query using the ordinal
position of the output column and the second query using the input
column name::

    SELECT count(*), nationkey FROM customer GROUP BY 2;

    SELECT count(*), nationkey FROM customer GROUP BY nationkey;

``GROUP BY`` clauses can group output by input column names not appearing in
the output of a select statement. For example, the following query generates
row counts for the ``customer`` table using the input column ``mktsegment``::

    SELECT count(*) FROM customer GROUP BY mktsegment;

.. code-block:: none

     _col0
    -------
     29968
     30142
     30189
     29949
     29752
    (5 rows)

When a ``GROUP BY`` clause is used in a ``SELECT`` statement all output
expressions must be either aggregate functions or columns present in
the ``GROUP BY`` clause.

.. _complex_grouping_operations:

**Complex Grouping Operations**

Presto also supports complex aggregations using the ``GROUPING SETS``, ``CUBE``
and ``ROLLUP`` syntax. This syntax allows users to perform analysis that requires
aggregation on multiple sets of columns in a single query. Complex grouping
operations do not support grouping on expressions composed of input columns.
Only column names or ordinals are allowed.

Complex grouping operations are often equivalent to a ``UNION ALL`` of simple
``GROUP BY`` expressions, as shown in the following examples. This equivalence
does not apply, however, when the source of data for the aggregation
is non-deterministic.

**GROUPING SETS**

Grouping sets allow users to specify multiple lists of columns to group on.
The columns not part of a given sublist of grouping columns are set to ``NULL``.
::

    SELECT * FROM shipping;

.. code-block:: none

     origin_state | origin_zip | destination_state | destination_zip | package_weight
    --------------+------------+-------------------+-----------------+----------------
     California   |      94131 | New Jersey        |            8648 |             13
     California   |      94131 | New Jersey        |            8540 |             42
     New Jersey   |       7081 | Connecticut       |            6708 |            225
     California   |      90210 | Connecticut       |            6927 |           1337
     California   |      94131 | Colorado          |           80302 |              5
     New York     |      10002 | New Jersey        |            8540 |              3
    (6 rows)

``GROUPING SETS`` semantics are demonstrated by this example query::

    SELECT origin_state, origin_zip, destination_state, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state),
        (origin_state, origin_zip),
        (destination_state));

.. code-block:: none

     origin_state | origin_zip | destination_state | _col0
    --------------+------------+-------------------+-------
     New Jersey   | NULL       | NULL              |   225
     California   | NULL       | NULL              |  1397
     New York     | NULL       | NULL              |     3
     California   |      90210 | NULL              |  1337
     California   |      94131 | NULL              |    60
     New Jersey   |       7081 | NULL              |   225
     New York     |      10002 | NULL              |     3
     NULL         | NULL       | Colorado          |     5
     NULL         | NULL       | New Jersey        |    58
     NULL         | NULL       | Connecticut       |  1562
    (10 rows)

The preceding query may be considered logically equivalent to a ``UNION ALL`` of
multiple ``GROUP BY`` queries::

    SELECT origin_state, NULL, NULL, sum(package_weight)
    FROM shipping GROUP BY origin_state

    UNION ALL

    SELECT origin_state, origin_zip, NULL, sum(package_weight)
    FROM shipping GROUP BY origin_state, origin_zip

    UNION ALL

    SELECT NULL, NULL, destination_state, sum(package_weight)
    FROM shipping GROUP BY destination_state;

However, the query with the complex grouping syntax (``GROUPING SETS``, ``CUBE``
or ``ROLLUP``) will only read from the underlying data source once, while the
query with the ``UNION ALL`` reads the underlying data three times. This is why
queries with a ``UNION ALL`` may produce inconsistent results when the data
source is not deterministic.

**CUBE**

The ``CUBE`` operator generates all possible grouping sets (i.e. a power set)
for a given set of columns. For example, the query::

    SELECT origin_state, destination_state, sum(package_weight)
    FROM shipping
    GROUP BY CUBE (origin_state, destination_state);

is equivalent to::

    SELECT origin_state, destination_state, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state, destination_state),
        (origin_state),
        (destination_state),
        ());

.. code-block:: none

     origin_state | destination_state | _col0
    --------------+-------------------+-------
     California   | New Jersey        |    55
     California   | Colorado          |     5
     New York     | New Jersey        |     3
     New Jersey   | Connecticut       |   225
     California   | Connecticut       |  1337
     California   | NULL              |  1397
     New York     | NULL              |     3
     New Jersey   | NULL              |   225
     NULL         | New Jersey        |    58
     NULL         | Connecticut       |  1562
     NULL         | Colorado          |     5
     NULL         | NULL              |  1625
    (12 rows)

**ROLLUP**

The ``ROLLUP`` operator generates all possible subtotals for a given set of
columns. For example, the query::

    SELECT origin_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY ROLLUP (origin_state, origin_zip);

.. code-block:: none

     origin_state | origin_zip | _col2
    --------------+------------+-------
     California   |      94131 |    60
     California   |      90210 |  1337
     New Jersey   |       7081 |   225
     New York     |      10002 |     3
     California   | NULL       |  1397
     New York     | NULL       |     3
     New Jersey   | NULL       |   225
     NULL         | NULL       |  1625
    (8 rows)

is equivalent to::

    SELECT origin_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS ((origin_state, origin_zip), (origin_state), ());

**Combining multiple grouping expressions**

Multiple grouping expressions in the same query are interpreted as having
cross-product semantics. For example, the following query::

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY
        GROUPING SETS ((origin_state, destination_state)),
        ROLLUP (origin_zip);

which can be rewritten as::

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY
        GROUPING SETS ((origin_state, destination_state)),
        GROUPING SETS ((origin_zip), ());

is logically equivalent to::

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state, destination_state, origin_zip),
        (origin_state, destination_state));

.. code-block:: none

     origin_state | destination_state | origin_zip | _col3
    --------------+-------------------+------------+-------
     New York     | New Jersey        |      10002 |     3
     California   | New Jersey        |      94131 |    55
     New Jersey   | Connecticut       |       7081 |   225
     California   | Connecticut       |      90210 |  1337
     California   | Colorado          |      94131 |     5
     New York     | New Jersey        | NULL       |     3
     New Jersey   | Connecticut       | NULL       |   225
     California   | Colorado          | NULL       |     5
     California   | Connecticut       | NULL       |  1337
     California   | New Jersey        | NULL       |    55
    (10 rows)

The ``ALL`` and ``DISTINCT`` quantifiers determine whether duplicate grouping
sets each produce distinct output rows. This is particularly useful when
multiple complex grouping sets are combined in the same query. For example, the
following query::

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY ALL
        CUBE (origin_state, destination_state),
        ROLLUP (origin_state, origin_zip);

is equivalent to::

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state, destination_state, origin_zip),
        (origin_state, origin_zip),
        (origin_state, destination_state, origin_zip),
        (origin_state, origin_zip),
        (origin_state, destination_state),
        (origin_state),
        (origin_state, destination_state),
        (origin_state),
        (origin_state, destination_state),
        (origin_state),
        (destination_state),
        ());

However, if the query uses the ``DISTINCT`` quantifier for the ``GROUP BY``::

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY DISTINCT
        CUBE (origin_state, destination_state),
        ROLLUP (origin_state, origin_zip);

only unique grouping sets are generated::

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state, destination_state, origin_zip),
        (origin_state, origin_zip),
        (origin_state, destination_state),
        (origin_state),
        (destination_state),
        ());

The default set quantifier is ``ALL``.

HAVING Clause
-------------

The ``HAVING`` clause is used in conjunction with aggregate functions and
the ``GROUP BY`` clause to control which groups are selected. A ``HAVING``
clause eliminates groups that do not satisfy the given conditions.
``HAVING`` filters groups after groups and aggregates are computed.

The following example queries the ``customer`` table and selects groups
with an account balance greater than the specified value::


    SELECT count(*), mktsegment, nationkey,
           CAST(sum(acctbal) AS bigint) AS totalbal
    FROM customer
    GROUP BY mktsegment, nationkey
    HAVING sum(acctbal) > 5700000
    ORDER BY totalbal DESC;

.. code-block:: none

     _col0 | mktsegment | nationkey | totalbal
    -------+------------+-----------+----------
      1272 | AUTOMOBILE |        19 |  5856939
      1253 | FURNITURE  |        14 |  5794887
      1248 | FURNITURE  |         9 |  5784628
      1243 | FURNITURE  |        12 |  5757371
      1231 | HOUSEHOLD  |         3 |  5753216
      1251 | MACHINERY  |         2 |  5719140
      1247 | FURNITURE  |         8 |  5701952
    (7 rows)

UNION | INTERSECT | EXCEPT Clause
---------------------------------

``UNION``  ``INTERSECT`` and ``EXCEPT`` are all set operations.  These clauses are used
to combine the results of more than one select statement into a single result set:

.. code-block:: none

    query UNION [ALL | DISTINCT] query

.. code-block:: none

    query INTERSECT [DISTINCT] query

.. code-block:: none

    query EXCEPT [DISTINCT] query

The argument ``ALL`` or ``DISTINCT`` controls which rows are included in
the final result set. If the argument ``ALL`` is specified all rows are
included even if the rows are identical.  If the argument ``DISTINCT``
is specified only unique rows are included in the combined result set.
If neither is specified, the behavior defaults to ``DISTINCT``.  The ``ALL``
argument is not supported for ``INTERSECT`` or ``EXCEPT``.


Multiple set operations are processed left to right, unless the order is explicitly
specified via parentheses. Additionally, ``INTERSECT`` binds more tightly
than ``EXCEPT`` and ``UNION``. That means ``A UNION B INTERSECT C EXCEPT D``
is the same as ``A UNION (B INTERSECT C) EXCEPT D``.

**UNION**

``UNION`` combines all the rows that are in the result set from the
first query with those that are in the result set for the second query.
The following is an example of one of the simplest possible ``UNION`` clauses.
It selects the value ``13`` and combines this result set with a second query
that selects the value ``42``::

    SELECT 13
    UNION
    SELECT 42;

.. code-block:: none

     _col0
    -------
        13
        42
    (2 rows)

The following query demonstrates the difference between ``UNION`` and ``UNION ALL``.
It selects the value ``13`` and combines this result set with a second query that
selects the values ``42`` and ``13``::

    SELECT 13
    UNION
    SELECT * FROM VALUES(42, 13);

.. code-block:: none

     _col0
    -------
        13
        42
    (2 rows)

::

    SELECT 13
    UNION ALL
    SELECT * FROM VALUES(42, 13);

.. code-block:: none

     _col0
    -------
        13
        42
        13
    (2 rows)

**INTERSECT**

``INTERSECT`` returns only the rows that are in the result sets of both the first and
the second queries. The following is an example of one of the simplest
possible ``INTERSECT`` clauses. It selects the values ``13`` and ``42`` and combines
this result set with a second query that selects the value ``13``.  Since ``42``
is only in the result set of the first query, it is not included in the final results.::

    SELECT * FROM VALUES (13, 42)
    INTERSECT
    SELECT 13;

.. code-block:: none

     _col0
    -------
        13
    (2 rows)

**EXCEPT**

``EXCEPT`` returns the rows that are in the result set of the first query,
but not the second. The following is an example of one of the simplest
possible ``EXCEPT`` clauses. It selects the values ``13`` and ``42`` and combines
this result set with a second query that selects the value ``13``.  Since ``13``
is also in the result set of the second query, it is not included in the final result.::

    SELECT * FROM VALUES (13, 42)
     EXCEPT
    SELECT 13;

.. code-block:: none

     _col0
    -------
       42
    (2 rows)

ORDER BY Clause
---------------

The ``ORDER BY`` clause is used to sort a result set by one or more
output expressions:

.. code-block:: none

    ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...]

Each expression may be composed of output columns or it may be an ordinal
number selecting an output column by position (starting at one). The
``ORDER BY`` clause is evaluated as the last step of a query after any
``GROUP BY`` or ``HAVING`` clause. The default null ordering is ``NULLS LAST``,
regardless of the ordering direction.

LIMIT Clause
------------

The ``LIMIT`` clause restricts the number of rows in the result set.
``LIMIT ALL`` is the same as omitting the ``LIMIT`` clause.
The following example queries a large table, but the limit clause restricts
the output to only have five rows (because the query lacks an ``ORDER BY``,
exactly which rows are returned is arbitrary)::

    SELECT orderdate FROM orders LIMIT 5;

.. code-block:: none

     o_orderdate
    -------------
     1996-04-14
     1992-01-15
     1995-02-01
     1995-11-12
     1992-04-26
    (5 rows)

TABLESAMPLE
-----------

There are multiple sample methods:

``BERNOULLI``
    Each row is selected to be in the table sample with a probability of
    the sample percentage. When a table is sampled using the Bernoulli
    method, all physical blocks of the table are scanned and certain
    rows are skipped (based on a comparison between the sample percentage
    and a random value calculated at runtime).

    The probability of a row being included in the result is independent
    from any other row. This does not reduce the time required to read
    the sampled table from disk. It may have an impact on the total
    query time if the sampled output is processed further.

``SYSTEM``
    This sampling method divides the table into logical segments of data
    and samples the table at this granularity. This sampling method either
    selects all the rows from a particular segment of data or skips it
    (based on a comparison between the sample percentage and a random
    value calculated at runtime).

    The rows selected in a system sampling will be dependent on which
    connector is used. For example, when used with Hive, it is dependent
    on how the data is laid out on HDFS. This method does not guarantee
    independent sampling probabilities.

.. note:: Neither of the two methods allow deterministic bounds on the number of rows returned.

Examples::

    SELECT *
    FROM users TABLESAMPLE BERNOULLI (50);

    SELECT *
    FROM users TABLESAMPLE SYSTEM (75);

Using sampling with joins::

    SELECT o.*, i.*
    FROM orders o TABLESAMPLE SYSTEM (10)
    JOIN lineitem i TABLESAMPLE BERNOULLI (40)
      ON o.orderkey = i.orderkey;

.. _unnest:

UNNEST
------

``UNNEST`` can be used to expand an :ref:`array_type` or :ref:`map_type` into a relation.
Arrays are expanded into a single column, and maps are expanded into two columns (key, value).
``UNNEST`` can also be used with multiple arguments, in which case they are expanded into multiple columns,
with as many rows as the highest cardinality argument (the other columns are padded with nulls).
``UNNEST`` can optionally have a ``WITH ORDINALITY`` clause, in which case an additional ordinality column
is added to the end.
``UNNEST`` is normally used with a ``JOIN`` and can reference columns
from relations on the left side of the join.

Using a single column::

    SELECT student, score
    FROM tests
    CROSS JOIN UNNEST(scores) AS t (score);

Using multiple columns::

    SELECT numbers, animals, n, a
    FROM (
      VALUES
        (ARRAY[2, 5], ARRAY['dog', 'cat', 'bird']),
        (ARRAY[7, 8, 9], ARRAY['cow', 'pig'])
    ) AS x (numbers, animals)
    CROSS JOIN UNNEST(numbers, animals) AS t (n, a);

.. code-block:: none

      numbers  |     animals      |  n   |  a
    -----------+------------------+------+------
     [2, 5]    | [dog, cat, bird] |    2 | dog
     [2, 5]    | [dog, cat, bird] |    5 | cat
     [2, 5]    | [dog, cat, bird] | NULL | bird
     [7, 8, 9] | [cow, pig]       |    7 | cow
     [7, 8, 9] | [cow, pig]       |    8 | pig
     [7, 8, 9] | [cow, pig]       |    9 | NULL
    (6 rows)

``WITH ORDINALITY`` clause::

    SELECT numbers, n, a
    FROM (
      VALUES
        (ARRAY[2, 5]),
        (ARRAY[7, 8, 9])
    ) AS x (numbers)
    CROSS JOIN UNNEST(numbers) WITH ORDINALITY AS t (n, a);

.. code-block:: none

      numbers  | n | a
    -----------+---+---
     [2, 5]    | 2 | 1
     [2, 5]    | 5 | 2
     [7, 8, 9] | 7 | 1
     [7, 8, 9] | 8 | 2
     [7, 8, 9] | 9 | 3
    (5 rows)

Joins
-----

Joins allow you to combine data from multiple relations.

CROSS JOIN
^^^^^^^^^^

A cross join returns the Cartesian product (all combinations) of two
relations. Cross joins can either be specified using the explit
``CROSS JOIN`` syntax or by specifying multiple relations in the
``FROM`` clause.

Both of the following queries are equivalent::

    SELECT *
    FROM nation
    CROSS JOIN region;

    SELECT *
    FROM nation, region;

The ``nation`` table contains 25 rows and the ``region`` table contains 5 rows,
so a cross join between the two tables produces 125 rows::

    SELECT n.name AS nation, r.name AS region
    FROM nation AS n
    CROSS JOIN region AS r
    ORDER BY 1, 2;

.. code-block:: none

         nation     |   region
    ----------------+-------------
     ALGERIA        | AFRICA
     ALGERIA        | AMERICA
     ALGERIA        | ASIA
     ALGERIA        | EUROPE
     ALGERIA        | MIDDLE EAST
     ARGENTINA      | AFRICA
     ARGENTINA      | AMERICA
    ...
    (125 rows)

Qualifying Column Names
^^^^^^^^^^^^^^^^^^^^^^^

When two relations in a join have columns with the same name, the column
references must be qualified using the relation alias (if the relation
has an alias), or with the relation name::

    SELECT nation.name, region.name
    FROM nation
    CROSS JOIN region;

    SELECT n.name, r.name
    FROM nation AS n
    CROSS JOIN region AS r;

    SELECT n.name, r.name
    FROM nation n
    CROSS JOIN region r;

The following query will fail with the error ``Column 'name' is ambiguous``::

    SELECT name
    FROM nation
    CROSS JOIN region;

Subqueries
----------

A subquery is an expression which is composed of a query. The subquery
is correlated when it refers to columns outside of the subquery.
Logically, the subquery will be evaluated for each row in the surrounding
query. The referenced columns will thus be constant during any single
evaluation of the subquery.

.. note:: Support for correlated subqueries is limited. Not every standard form is supported.

EXISTS
^^^^^^

The ``EXISTS`` predicate determines if a subquery returns any rows::

    SELECT name
    FROM nation
    WHERE EXISTS (SELECT * FROM region WHERE region.regionkey = nation.regionkey)

IN
^^

The ``IN`` predicate determines if any values produced by the subquery
are equal to the provided expression. The result of ``IN`` follows the
standard rules for nulls. The subquery must produce exactly one column::

    SELECT name
    FROM nation
    WHERE regionkey IN (SELECT regionkey FROM region)

Scalar Subquery
^^^^^^^^^^^^^^^

A scalar subquery is a non-correlated subquery that returns zero or
one row. It is an error for the subquery to produce more than one
row. The returned value is ``NULL`` if the subquery produces no rows::

    SELECT name
    FROM nation
    WHERE regionkey = (SELECT max(regionkey) FROM region)

.. note:: Currently only single column can be returned from the scalar subquery.
