============
Aggregations
============

This article discusses aggregation-related optimizations in Velox. We go through
the different techniques and provide examples and define the conditions for the
application of each.

Velox supports partial and final aggregations with zero, one or multiple
grouping keys and zero, one or multiple aggregate functions.

:doc:`Aggregate Functions <../functions/presto/aggregate>` section of the documentation
lists all available aggregate functions and :doc:`How to add an aggregate
function? <aggregate-functions>` guide explains how to add more.

Use AggregationNode to insert an aggregation into the query plan. Specify
aggregation step (partial, intermediate, final, or single), grouping keys and
aggregate functions. You may also specify boolean columns to mask out rows for
some or all aggregations. Grouping keys must refer to input columns and cannot
contain expressions. To compute aggregations over expressions add ProjectNode
just before the AggregationNode.

Here are examples of aggregation query plans:

Group-by with a single grouping key and single aggregate function:

.. code-block:: sql

    SELECT a, sum(b) FROM t GROUP BY 1

* AggregationNode: groupingKeys = {a}, aggregates = {sum(b)}

Group-by with a single grouping key and an aggregate function applied to an
expression:

.. code-block:: sql

    SELECT a, sum(b * c) FROM t GROUP BY 1

* AggregationNode: groupingKeys = {a}, aggregates = {sum(d)}
    * ProjectNode: a, d := b * c

Group-by with multiple grouping keys and multiple aggregates:

.. code-block:: sql

    SELECT a, b, sum(c), avg(c) FROM t GROUP BY 1, 2

* AggregationNode: groupingKeys = {a, b}, aggregates = {sum(c), avg(c)}

Distinct aggregation:

.. code-block:: sql

    SELECT DISTINCT a, b FROM t

* AggregationNode: groupingKeys = {a, b}, aggregates = {}

Aggregation with a mask:

.. code-block:: sql

    SELECT a, sum (b) filter (where c > 10) FROM t GROUP BY 1

* AggregationNode: groupingKeys = {a}, aggregates = {sum(b, mask: d)}
    * ProjectNode: a, b, d := c > 10

Global aggregation:

.. code-block:: sql

    SELECT sum(a), avg(b) FROM t

* AggregationNode: groupingKeys = {}, aggregates = {sum(a), avg(b)}

HashAggregation Operator
------------------------

AggregationNode is translated to the HashAggregation operator for execution.
Distinct aggregations, e.g. aggregations with no aggregates, run in streaming
mode. For each batch of input rows, the operator determines a set of new
grouping key values and returns these as results. Aggregations with one or more
aggregate functions need to process all input before producing the results.

Push-Down into Table Scan
-------------------------

HashAggregation operator supports pushing down aggregations into table scan.
Pushdown is enabled when all of the following conditions are met:

* the aggregation function takes a single argument, 
* the argument is a column read directly from the table without any transformations, 
* that column is not used anywhere else in the query. 

For example, pushdown is possible in the following query:

.. code-block:: sql

    SELECT a, sum(b) FROM t GROUP BY 1

Pushdown is also possible if the data is filtered using columns other than the
column that is the input to the aggregation function. For example, pushdown is
enabled in the following query:

.. code-block:: sql

    SELECT a, sum(b) 
    FROM t 
    WHERE a > 100 
    GROUP BY 1

In these queries, TableScan operator produces "b" column as a LazyVector
and "sum" aggregate function loads this vector using ValueHook, e.g. each value
is read from the file and passed directly to "sum" aggregate which adds it to
the accumulator. No intermediate vector is produced in this case.

The following aggregate functions support pushdown: :func:`sum`, :func:`min`,
:func:`max`, :func:`bitwise_and_agg`, :func:`bitwise_or_agg`, :func:`bool_and`,
:func:`bool_or`.

Adaptive Array-Based Aggregation
--------------------------------

HashAggregation operator stores aggregated data in rows. Each row corresponds to
a unique combination of grouping key values. Global aggregations store data in
a single row. Check out the Memory Layout section of :doc:`How to add an aggregate
function? <aggregate-functions>` guide for details.

Data rows are organized into a hash table which can be in either hash, array or
normalized key mode.

Hash mode
~~~~~~~~~

In hash mode, the processing of incoming rows consists of the following steps:

* calculate a hash of the grouping keys,
* use that hash to look up one or more possibly matching entries in the hash table,
* compare the grouping keys to identify the single matching entry or determine that no such entry exists,
* insert a new entry if a matching entry doesn’t exist,
* update the accumulators of an existing or newly created entry.

Array mode
~~~~~~~~~~

In array mode, there is an array of pointers to data rows. The grouping key
values of the incoming rows are mapped to a single integer which is used as an
index into the array. Entries with no matching grouping keys store nullptr.

Consider SELECT a, sum(b) FROM t GROUP BY 1 query over the following data:

==  ==
a   b
==  ==
1   10
7   12
1   4
4   128
10  -29
7   3
==  ==

There is a single grouping key, a, with values from a small integer range:
[1, 10]. In array mode, hash table allocates an array of size 10 and maps
grouping key values to an index into an array using a simple formula: index =
a - 1.

Initially, the array is filled with nulls: [null, null, … null]. As rows are processed entries get populated.

============================================    =========================================================
After adding the first row {1, 10}:             [10, null, null, null, null, null, null, null, null, null]
After adding the second row {7, 12}:            [10, null, null, null, null, null, 12, null, null, null]
After adding the third row {1, 4}:              [14, null, null, null, null, null, 12, null, null, null]
After adding the 4th row {4, 128}               [10, null, null, 128, null, null, 12, null, null, null]
After adding the 5th row {10, -29}:             [10, null, null, null, null, null, 12, null, null, -29]
After adding the last row {7, 3}:               [10, null, null, null, null, null, 15, null, null, -29]
============================================    =========================================================

Compared with hash mode, array mode is very efficient as it doesn’t require
computing the hash and comparing the incoming grouping keys with hash table
entries. Unlike hash mode which can be used for any aggregation, array mode
applies only when the values of the grouping keys can be mapped to a relatively
small integer range. For example, this is the case when there is a single
grouping key of integer type and the difference between minimum and maximum
values is relatively small. In this case, the mapping formula is simple: ``index
= value - min``.

Array mode also applies when there are two or more grouping keys and the
multiple of their value ranges is still small. For example, GROUP BY a, b
with "a" values from [10, 50] range and "b" values from [1000, 1050] range
allows for array mode with array size equal to 40 * 50 = 200 and mapping
formula: ``index = (a - 10) + (b - 1000) * 40``.

Furthermore, array mode applies when the number of unique values for a grouping
key is small. In this case, each unique value can be assigned an ordinal number
starting from 1 (0 is reserved for null value) and that number can be used as
an index into the array.

Array mode also applies to a mix of grouping keys with small value ranges and
small number of unique values as long as the product of value range sizes and
number of unique values doesn’t exceed maximum value allowed for the array
mode.

Array mode supports arrays up to 2M entries.

Array mode trivially applies to grouping keys of type boolean since there are
only 3 possible values: null, false, true. These are mapped to 0, 1, 2
respectively.

Grouping keys that are short strings, up to 7 bytes, are mapped to 64-bit
integers by padding with leading zeros and placing 1 in the first bit before
the string bytes, e.g. 00...01<string bytes>. If the resulting numbers fit in a
small range or if there is a small number of unique values, array mode is used.
Otherwise, the resulting number could be used in normalized key mode.

The integer values used to represent the grouping key values are referred to as
value IDs.

Normalized Key Mode
~~~~~~~~~~~~~~~~~~~

In normalized key mode, multiple grouping key values are mapped to a single
64-bit integer and the processing continues as in hash mode with a single
64-bit integer grouping key. This mode is less efficient than array mode, but
is more efficient than hash mode because hashing and comparing a single 64-bit
integer value is faster than hashing and comparing multiple values.

Adaptivity
~~~~~~~~~~

Hash table mode is decided adaptively starting with array mode and switching to
normalized key or hash mode if the new values of the grouping keys require
that. When switching modes the hash table needs to be re-organized. Once in
hash mode, the hash table stays in that mode for the rest of the query
processing.

For each grouping key, HashAggregation operator creates an instance of
VectorHasher to analyze and accumulate statistics about that key. VectorHasher
stores minimum and maximum values of the key. If the range grows too large,
VectorHasher switches to tracking the set of unique values. If the number of
unique values exceeds 100K, VectorHasher stops tracking these and the hash
table switches to normalized key or hash mode.

Array and normalized key modes are supported only for grouping keys of the
following types: boolean, tinyint, smallint, integer, bigint, varchar.
