***************
Join Reordering
***************

Background
----------

The order in which joins are executed in a query can have a significant impact on the query's performance. The aspect of
join ordering that has the largest impact on performance is the size of the data being processed and passed over the
network. If a join is not a primary key-foreign key join, the data produced can be much greater than the size of either
table in the join-- up to \|Table 1\| x \|Table 2\| for a cross join. If a join that produces a lot of data is performed
early in the execution, then subsequent stages will need to process large amounts of data for longer than necessary,
increasing the time and resources needed for the query.

Manual Join Reordering
----------------------

By default, Presto joins tables in the order in which they are listed in a query. It is the responsibility of the user to
optimize the join order when writing queries in order to achieve better performance and handle larger joins. It is often a
good idea to join small tables early in the plan, and leave larger fact tables until the end. One should also be careful of
introducing cross joins or join conditions that produce output that is larger than the size of the input. Those such joins
may sometimes be necessary or optimal, when introduced unintentionally, they can dramatically reduce the performance
of the query.

Cross Join Elimination
----------------------

When the configuration property ``reorder-joins`` or the session property ``reorder_joins`` is enabled, the optimizer will
search for cross joins in the query plan and try to eliminate them by changing the join order. When reordering, it will
try to preserve the original join order as much as possible. If cross joins cannot be eliminated, the original join order
will be maintained. For this optimization, the optimizer does not use any statistics. It will try to eliminate any cross
join it can, even if including the cross joins would have resulted in a more optimal query plan. For example, it may be
optimal to perform a cross join of two small dimension tables before joining in the larger fact table. However, the
optimizer will nevertheless reorder the joins to remove the cross join. Because of this limitation, this property should be
used cautiously (note that a user can instead reorder the joins manually to achieve the same effect). The ``reorder-joins``
property is set to ``false`` by default.

Examples
^^^^^^^^
For the following query:

.. code-block:: sql

  SELECT * FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey;

In the original join order ``part, orders, lineitem``, Presto will first join the ``part`` table with the
``orders`` table, for which there is no join condition. When ``reorder-joins=true``, the join order will be changed
to ``part, lineitem, orders`` to eliminate the cross join.

For the following query:

.. code-block:: sql

  SELECT * FROM part p, orders o, lineitem l, supplier s, nation n
  WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND l.suppkey = s.suppkey AND s.nationkey = n.nationkey;

The join order will change from ``part, orders, lineitem, supplier, nation`` to ``part, lineitem, orders, supplier, nation``.
