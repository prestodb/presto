==========
Anti joins
==========

Anti joins can be used to efficiently implement queries with NOT IN <subquery>
and NOT EXISTS <subquery> clauses. The semantics of these queries are different
in the presence of NULLs in the outer query or subquery. NOT IN semantics are
implemented by the null aware anti join. NOT EXISTS semantics are implemented
by the regular anti join.

Currently, Velox provides only null aware anti join via the JoinType::kAnti.
Regular anti join is not available. We will rename kAnti to kNullAwareAnti for
clarity and introduce a new kAnti join type for the regular anti join.

This article explains the differences in semantics between NOT IN and NOT EXISTS
queries and discusses the implementation of these in the null aware and regular
anti joins.

NOTE: Presto optimizer doesn’t plan NOT IN and NOT EXISTS queries using anti
joins. To compensate, query plan translation in Prestissimo detects a NOT IN
pattern and `converts <https://github.com/prestodb/presto/blob/master/presto-native-execution/presto_cpp/main/types/PrestoToVeloxQueryPlan.cpp#L1031>`_
it into Velox anti join plan node.

NOTE: `Substrait <https://substrait.io/relations/logical_relations/#join-types>`_
currently defines only one type of anti join and doesn’t specify
whether the semantics are NOT IN or NOT EXISTS. This topic is in active
`discussion <https://github.com/substrait-io/substrait/issues/325>`_.

It is easiest to understand the semantic differences using examples.

Consider table t:

==== =====
id   value
==== =====
NULL 0
1    1
2    2
==== =====

and table u:

==== =====
id   value
==== =====
NULL 0
2    2
3    3
==== =====


When experimenting with queries, it is convenient to use WITH clause combined
with UNNEST clause to create temporary datasets. Here is how to create and use
the datasets above:

.. code-block:: sql

	WITH
	     t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
	     u as (SELECT * FROM unnest(array[null, 2, 3], array[0, 2, 3]) as _t(id, value))
	<query that uses t and u>

	> SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value);

	  id  | value
	------+-------
	 NULL |     0
	    1 |     1
	    2 |     2
	(3 rows)

	> SELECT * FROM unnest(array[null, 2, 3], array[0, 2, 3]) as _t(id, value);

	  id  | value
	------+-------
	 NULL |     0
	    2 |     2
	    3 |     3
	(3 rows)

	> WITH
	     t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
	     u as (SELECT * FROM unnest(array[null, 2, 3], array[0, 1, 2]) as _t(id, value))
	SELECT * FROM t FULL JOIN u ON t.id = u.id;

	  id  | value |  id  | value
	------+-------+------+-------
	    1 |     1 | NULL | NULL
	    2 |     2 |    2 |     1
	 NULL | NULL  |    3 |     2
	 NULL |     0 | NULL | NULL
	 NULL | NULL  | NULL |     0
	(5 rows)


NOT IN <subquery> Semantics
---------------------------

We need to consider 3 cases of NOT IN queries:

* Subquery returns a row with a NULL.
* Subquery returns no rows.
* Subquery returns one or more rows and no row has a NULL.

NOT IN with NULLs in the subquery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Consider NOT IN query:

.. code-block:: sql

	SELECT * FROM t WHERE t.id NOT IN (SELECT * FROM u)

This query returns an empty result.

.. code-block:: sql

	> WITH
	     t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
	     u as (SELECT * FROM unnest(array[null, 2, 3], array[0, 2, 3]) as _t(id, value))
	SELECT * from t WHERE t.id NOT IN (SELECT id FROM u);

	 id | value
	----+-------
	(0 rows)


This is because the IN LIST contains 3 values: NULL, 2, 3. In SQL, NULL is
considered an unknown value. In this case the IN LIST contains unknown value
and we cannot definitively say whether any given value is in the list or not.
Therefore NOT IN predicate returns a NULL (unknown), hence, the query returns
no result. You can use the following queries to confirm the semantics of the
NOT IN predicate.

.. code-block:: sql

	> SELECT 1 not in (null, 2, 3);

	 _col0
	-------
	 NULL
	(1 row)

	> SELECT null not in (null, 2, 3);

	 _col0
	-------
	 NULL
	(1 row)

NOT IN without NULLs in the subquery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, consider NOT IN query where subquery doesn’t return NULLs (by removing
NULLs from the u table or adding u.id IS NOT NULL predicate to the subquery).


.. code-block:: sql

	SELECT * FROM t WHERE t.id NOT IN (
		SELECT * FROM u WHERE u.id IS NOT NULL
	)

This query returns a single row with id 1.

.. code-block:: sql

	> WITH
	     t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
	     u as (SELECT * FROM unnest(array[2, 3], array[1, 2]) as _t(id, value))
	SELECT * from t WHERE t.id NOT IN (SELECT id FROM u);

	 id | value
	----+-------
	  1 |     1
	(1 row)

In this case the IN LIST contains 2 values: 2 and 3. NULL NOT IN (2, 3) returns
NULL because we cannot tell definitively whether an unknown value is part of a
set or not, hence, not included in the results. 1 NOT IN (2, 3) returns true,
hence, included in the results. 2 NOT IN (2, 3) returns false, hence, not
included in the results.

NOT IN with empty subquery
~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, consider a NOT IN query with a subquery that returns empty results
(by removing all rows from the u table or adding an always false predicate to
the subquery).

.. code-block:: sql

	SELECT * FROM t WHERE t.id NOT IN (
		SELECT * FROM u WHERE u.id < 0
	)

This query returns all rows from t, including the row with NULL id.

.. code-block:: sql

    > WITH
        t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
        u as (SELECT * FROM unnest(array[], array[]) as _t(id, value))
    SELECT * from t WHERE t.id NOT IN (SELECT id FROM u);

	  id  | value
	------+-------
	    1 |     1
	    2 |     2
	 NULL |     0
	(3 rows)

Here, the IN LIST is empty. Hence, all values, including unknown value
(NULL), can be determined to be not part of that set.

NOT EXISTS <subquery> Semantics
-------------------------------

Similar to NOT IN queries, we consider 3 cases:

* Subquery returns a row with a NULL.
* Subquery returns no rows.
* Subquery returns one or more rows and no row has a NULL.

NOT EXISTS with NULLs in the subquery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Consider NOT EXISTS query:

.. code-block:: sql

	SELECT * FROM t WHERE NOT EXISTS (SELECT id FROM u WHERE u.id = t.id)

This query returns 2 rows with IDs NULL and 1.

.. code-block:: sql

	> WITH
	     t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
	     u as (SELECT * FROM unnest(array[null, 2, 3], array[0, 1, 2]) as _t(id, value))
	SELECT * from t WHERE NOT EXISTS (SELECT * FROM u WHERE u.id = t.id);

	  id  | value
	------+-------
	 NULL |     0
	    1 |     1
	(2 rows)

Here, we have a correlated subquery, e.g. subquery that includes columns from
the outer query. This subquery returns different results for different outer
query rows.

For the row with id NULL, the sub-query is

.. code-block:: sql

	SELECT * FROM u WHERE u.id = NULL


u.id = NULL predicate always returns NULL, hence, the subquery returns an empty
result, hence, NOT EXISTS <subquery> clause evaluates to true.

For the row with id 1, the sub-query is

.. code-block:: sql

	SELECT * FROM u WHERE u.id = 1

u.id = 1 evaluates to NULL when u.id is null and false when u.id is 2 or 3.
Hence, the subquery results are empty, hence, NOT EXISTS <subquery> clause
evaluate to true.

For the row with id 2, the sub-query is

.. code-block:: sql

	SELECT * FROM u WHERE u.id = 2

u.id = 2 predicate evaluates to true for the row where u.id is 2, hence, the
subquery results are not empty, hence, the NOT EXISTS <subquery> clause
evaluates to false.

NOT EXISTS without NULLs in the subquery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, consider NOT EXISTS query with no nulls in the subquery:

.. code-block:: sql

	SELECT * FROM t WHERE NOT EXISTS (
		SELECT id FROM u WHERE u.id = t.id AND u.id IS NOT NULL
	)

This query returns 2 rows with IDs NULL and 1. In fact, the presence of NULLs in
the subquery doesn’t affect the results of the NOT EXISTS clause. This is
because u.id = t.id predicate evaluates to NULL when u.id is NULL, hence, rows
with NULLs are excluded from the subquery. Unlike the NOT IN query, NOT EXISTS
query is not sensitive for NULLs in the subquery.


.. code-block:: sql

    > WITH
        t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
        u as (SELECT * FROM unnest(array[2, 3], array[1, 2]) as _t(id, value))
    SELECT * from t WHERE NOT EXISTS (SELECT * FROM u WHERE u.id = t.id);

	  id  | value
	------+-------
	    1 |     1
	 NULL |     0
	(2 rows)

NOT EXISTS with empty subquery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, consider a NOT EXISTS query with a subquery that returns empty results.

.. code-block:: sql

	SELECT * FROM t WHERE NOT EXISTS (
		SELECT id FROM u WHERE u.id = t.id AND u.id < 0
	)

This query returns all rows from t because subquery always returns an empty
result set. When subquery is empty, the results of NOT IN and NOT EXISTS
queries are the same.

.. code-block:: sql

    > WITH
        t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
        u as (SELECT * FROM unnest(array[], array[]) as _t(id, value))
    SELECT * from t WHERE NOT EXISTS (SELECT * FROM u WHERE u.id = t.id);

	  id  | value
	------+-------
	    2 |     2
	    1 |     1
	 NULL |     0
	(3 rows)

Implementation
--------------

NOT IN and NOT EXISTS queries can be implemented efficiently using anti joins.
NOT IN queries are implemented using NULL AWARE ANTI JOIN. NOT EXISTS queries
are implemented using regular ANTI JOIN.

NULL AWARE ANTI JOIN
~~~~~~~~~~~~~~~~~~~~

NULL AWARE ANTI JOIN is used to implement NOT IN queries.

.. code-block:: sql

	SELECT * FROM t WHERE t.id NOT IN (SELECT id FROM u)

The rows from table t are placed on the left side of the join. The rows from the
subquery are placed on the right side of the join. The subquery rows are loaded
into a hash table keyed on “id”. If a NULL is encountered when building the
hash table, the join finishes early with no results. If the hash table is
empty (i.e. subquery returns no results), the join returns all the rows from
the left side including rows with NULL join key. If the hash table is not empty
and has no NULLs, the rows from the left side with no NULLs in the join key are
processed in streaming fashion. For each row, the join looks up a match in the
hash table and returns the row only if there is no match. Rows from the left
side with NULL in the join key are not returned.

This algorithm extends trivially to multiple join keys and NOT IN queries that look like this:

.. code-block:: sql

	SELECT * FROM t WHERE (t.id1, t.id2) NOT IN (SELECT id1, id2 FROM u)

To summarize, NULL AWARE ANTI JOIN semantics include

* Return empty results when the right side contains nulls in the join keys.
* Return left-side rows with NULLs in the join key only when the right side is empty.

In a distributed setup, evaluating the above conditions requires that every node
knows whether the combined right side is empty or not and whether it contains a
row with a null in the join key. This information is available if the query
broadcasts the right side or uses replicate-nulls-and-any partitioning
strategy.

NOTE: Replicate-null-and-any partitioning strategy replicates all rows with
nulls in the partition-by keys to all destinations and also replicates one
arbitrary chosen row with no nulls in the partition-by keys.

ANTI JOIN
~~~~~~~~~

Regular ANTI JOIN is used to implement NOT EXISTS queries.

.. code-block:: sql

	SELECT * FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.id = t.id)

First, we rewrite the subquery to return the equi-join clause u.id = t.id.

The rows from table t are placed on the left side of the join. The rows from the
modified subquery are placed on the right side of the join. The subquery rows
are loaded into a hash table keyed on “id”. Subquery rows with NULL join keys
are skipped. If the hash table is empty (i.e. subquery returns no results or
all results have NULLs in join keys), the join returns all the rows from the
left side including rows with NULL join key. This logic is the same between
regular ANTI JOIN and NULL AWARE ANTI JOIN. If the hash table is not empty, the
rows from the left side are processed in streaming fashion. All rows with NULL
in the join key are included in the results unconditionally. For each row with
non-NULL join key, the join looks up a match in the hash table and returns the
row only if there is no match.

This algorithm extends trivially to multiple join keys and NOT EXISTS queries
that look like this:

.. code-block:: sql

	SELECT * FROM t WHERE NOT EXISTS (
		SELECT * FROM u WHERE u.id1 = t.id1 AND u.id2 = t.id2
	)

The differences between regular and null aware anti join can be summarized as

* Regular join doesn’t automatically return empty results when the right side
  has NULLs in the join keys.
* Regular join unconditionally returns left side rows with NULLs in the join
  keys.

ANTI JOINs with Extra Filter
----------------------------

NOT IN and NOT EXISTS queries may contain non-equality conditions that use
columns from the outer query in the subqueries. For example,

.. code-block:: sql

	SELECT * FROM t WHERE t.id NOT IN (SELECT id FROM u WHERE u.value > t.value)

or

.. code-block:: sql

	SELECT * FROM t WHERE NOT EXISTS (
		SELECT * FROM u WHERE u.id = t.id AND u.value > t.value
	)

In this case, whether the subquery contains NULL in the join key or not depends
on values of the outer row and therefore can be different for different outer
rows. Hence, a row with a null in the join key on the right side, doesn’t
automatically make the null aware anti join return empty results.

This can be seen in an example.

.. code-block:: sql

	> WITH
	     t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
	     u as (SELECT * FROM unnest(array[null, 2, 3], array[0, 1, 2]) as _t(id, value))
	SELECT * from t WHERE t.id NOT IN (SELECT id FROM u WHERE u.value > t.value);

	 id | value
	----+-------
	  1 |     1
	  2 |     2
	(2 rows)

In this query, subquery for row with NULL id is

.. code-block:: sql

	SELECT id FROM u WHERE u.value > 0

This subquery returns rows with ids 2 and 3. Row with NULL id hash value equal
to 0 and doesn’t pass u.value > 0 predicate. NULL NOT IN (2, 3) returns NULL,
hence, NULL row from the left side is not included in the query result.

Subquery for row with id = 1 is

.. code-block:: sql

	SELECT id FROM u WHERE u.value > 1

This subquery returns two rows with ids 2 and 3. 1 NOT IN (2, 3) returns true,
hence, row with id 1 is included in the query results.

Subquery for row with id = 2 is

.. code-block:: sql

	SELECT id FROM u WHERE u.value > 2

This subquery returns a single row with id 3. 2 NOT IN (3) returns true, hence,
row with id 2 is included in the query results.

Let’s consider a different example where results include the NULL row.

.. code-block:: sql

	> WITH
	     t as (SELECT * FROM unnest(array[null, 1, 2], array[0, 1, 2]) as _t(id, value)),
	     u as (SELECT * FROM unnest(array[null, 2, 3], array[0, 1, 2]) as _t(id, value))
	 SELECT * from t WHERE t.id NOT IN (SELECT id FROM u WHERE u.value * t.value > 0);

	  id  | value
	------+-------
	 NULL |     0
	    1 |     1
	(2 rows)

This query returns the row with NULL. The subquery for that row is:

.. code-block:: sql

	SELECT id FROM u WHERE u.value * 0 > 0

The predicate evaluates to false for all rows in u, hence, the IN LIST is empty,
hence NULL NOT IN <subquery> evaluates to true.

These queries are implemented using anti joins with extra filters. In the
examples above, the implementations use null aware anti joins with extra
filters u.value > t.value and u.value * t.value > 0.

The presence of extra filters changes the implementation of anti join.

NULL AWARE ANTI JOIN with Filter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the presence of an extra filter, null aware anti join cannot finish early
upon encountering a null in the join key on the right side. The join must
finish building the hash table and include all rows, even the ones with nulls
in the join key.

When evaluating left-side rows, the join needs to first collect all matches from
the build side and combine these with all the right-side rows with nulls in the
join key, then evaluate the filter on the matches. If the filter comes out
empty, the row is included in the results. Otherwise, the row is not included.

A more detailed description of this logic goes like this.

#. Collect the matches.
	#. If the left side row doesn’t have a null in the join key, include all matches from the right side.
	#. If the left side row has a null in the join key, include all rows from the right side.
	#. For all left side rows, include all rows from the right side with nulls in the join keys.
#. Evaluate the filter on the matches collected in the previous step.
	#. Include the left-side row in the results only If the filter comes out empty.

Step 1.2 requires evaluating the filter on the cross join of left-side rows with
nulls in the join key with all the right-side rows. This implies that in a
distributed setup the right side must be replicated (broadcasted) to all the
nodes evaluating the join, while the left side can be distributed among nodes
using any convenient strategy.

Step 1.3 requires that right-side rows with nulls in the join keys are
replicated (broadcasted) to all the nodes evaluating the join. This is achieved
using replicate-nulls-and-any partitioning strategy.

ANTI JOIN with Filter
~~~~~~~~~~~~~~~~~~~~~

In the presence of an extra filter, regular anti join can still unconditionally
return left side rows with nulls in the join key. The subquery with an extra
filter still returns an empty result for these rows.

For the left-side row with no nulls in the join key, the join needs to collect
the matches from the right side. If there are no matches, the row is included
in the results. If there are matches, the extra filter needs to be evaluated.
If the filter comes out empty, the row is included in the results.

Summary
-------

Velox currently provides null aware anti join via the JoinType::kAnti. Regular
anti join is not available. There is also a bug in filter processing where the
join always returns empty results if there is a build side row with null in the
join key.

To provide full support for efficient execution of NOT IN and NOT EXISTS
queries, we will rename kAnti to kNullAwareAnti and introduce a new kAnti join
type for the regular anti join. We will also fix the bug in null aware anti
join with filter.
