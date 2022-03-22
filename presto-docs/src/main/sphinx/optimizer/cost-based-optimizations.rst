========================
Cost based optimizations
========================

Presto supports several cost based optimizations, described below.

Join Enumeration
----------------

The order in which joins are executed in a query can have a significant impact
on the query's performance. The aspect of join ordering that has the largest
impact on performance is the size of the data being processed and transferred
over the network. If a join that produces a lot of data is performed early in
the execution, then subsequent stages will need to process large amounts of
data for longer than necessary, increasing the time and resources needed for
the query.

With cost based join enumeration, Presto uses
cdoc:`/optimizer/statistics` provided by connectors to estimate
the costs for different join orders and automatically pick the
join order with the lowest computed costs.

The join enumeration strategy is governed by the ``join_reordering_strategy``
session property, with the ``optimizer.join-reordering-strategy``
configuration property providing the default value.

The valid values are:
 * ``AUTOMATIC`` - full automatic join enumeration enabled
 * ``ELIMINATE_CROSS_JOINS`` (default) - eliminate unnecessary cross joins
 * ``NONE`` - purely syntactic join order

If using ``AUTOMATIC`` and statistics are not available, or if for any other
reason a cost could not be computed, the ``ELIMINATE_CROSS_JOINS`` strategy is
used instead.

Join Distribution Selection
---------------------------

Presto uses a hash based join algorithm. That implies that for each join
operator a hash table must be created from one join input (called build side).
The other input (probe side) is then iterated and for each row the hash table is
queried to find matching rows.

There are two types of join distributions:
 * Partitioned: each node participating in the query builds a hash table
   from only a fraction of the data
 * Broadcast: each node participating in the query builds a hash table
   from all of the data (data is replicated to each node)

Each type have its trade offs. Partitioned joins require redistributing both
tables using a hash of the join key. This can be slower (sometimes
substantially) than broadcast joins, but allows much larger joins. In
particular, broadcast joins will be faster if the build side is much smaller
than the probe side. However, broadcast joins require that the tables on the
build side of the join after filtering fit in memory on each node, whereas
distributed joins only need to fit in distributed memory across all nodes.

With cost based join distribution selection, Presto automatically chooses whether to
use a partitioned or broadcast join. With both cost based join enumeration and cost based join distribution, Presto
automatically chooses which side is the probe and which is the build.

The join distribution type is governed by the ``join_distribution_type``
session property, with the ``join-distribution-type`` configuration
property providing the default value.

The valid values are:
 * ``AUTOMATIC`` - join distribution type is determined automatically
   for each join
 * ``BROADCAST`` - broadcast join distribution is used for all joins
 * ``PARTITIONED`` (default) - partitioned join distribution is used for all join

Capping replicated table size
-----------------------------

Join distribution type will be chosen automatically when join reordering strategy
is set to ``COST_BASED`` or when join distribution type is set to ``AUTOMATIC``.
In such case it is possible to cap the maximum size of replicated table via
``join-max-broadcast-table-size`` config property (e.g. ``join-max-broadcast-table-size=100MB``)
or via ``join_max_broadcast_table_size`` session property (e.g. ``set session join_max_broadcast_table_size='100MB';``)
This allows improving cluster concurrency and prevents bad plans when CBO misestimates size of joined tables.

By default replicated table size is capped to 100MB.

Connector Implementations
-------------------------

In order for the Presto optimizer to use the cost based strategies,
the connector implementation must provide :doc:`statistics`.
