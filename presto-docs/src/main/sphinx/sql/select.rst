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
    [ LIMIT [ count | ALL ] ]

where ``from_item`` is one of

.. code-block:: none

    table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]

.. code-block:: none

    from_item join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]

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
groups of rows containing matching values. A ``GROUP BY`` clause may
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
expression must be either aggregate functions or columns present in
the ``GROUP BY`` clause.

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

UNION Clause
------------

The ``UNION`` clause is used to combine the results of more than one
select statement into a single result set:

.. code-block:: none

    query UNION [ALL | DISTINCT] query

The argument ``ALL`` or ``DISTINCT`` controls which rows are included in
the final result set. If the argument ``ALL`` is specified all rows are
included even if the rows are identical.  If the argument ``DISTINCT``
is specified only unique rows are included in the combined result set.
If neither is specified, the behavior defaults to ``DISTINCT``.

The following is an example of one of the simplest possible ``UNION``
clauses. The following query selects the value ``13`` and combines
this result set with a second query which selects the value ``42``::

    SELECT 13
    UNION
    SELECT 42;

.. code-block:: none

     _col0
    -------
        13
        42
    (2 rows)

Multiple unions are processed left to right, unless the order is explicitly
specified via parentheses.

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
