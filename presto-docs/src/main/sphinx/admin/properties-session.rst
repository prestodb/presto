=========================
Presto Session Properties
=========================

This section describes session properties that may be used to tune 
Presto or alter its behavior when required.

The following is not a complete list of all session properties 
available in Presto, and does not include any connector-specific 
catalog properties. 

For information on catalog properties, see the :doc:`connector documentation </connector/>`.

For information on configuration properties, see :doc:`properties`.


.. contents::
    :local:
    :backlinks: none
    :depth: 1

General Properties
------------------

``join_distribution_type``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Allowed values:** ``AUTOMATIC``, ``PARTITIONED``, ``BROADCAST``
* **Default value:** ``AUTOMATIC``

The type of distributed join to use.  When set to ``PARTITIONED``, presto will
use hash distributed joins.  When set to ``BROADCAST``, it will broadcast the
right table to all nodes in the cluster that have data from the left table.
Partitioned joins require redistributing both tables using a hash of the join key.
This can be slower (sometimes substantially) than broadcast joins, but allows much
larger joins. In particular broadcast joins will be faster if the right table is
much smaller than the left.  However, broadcast joins require that the tables on the right
side of the join after filtering fit in memory on each node, whereas distributed joins
only need to fit in distributed memory across all nodes. When set to ``AUTOMATIC``,
Presto will make a cost based decision as to which distribution type is optimal.
It will also consider switching the left and right inputs to the join.  In ``AUTOMATIC``
mode, Presto will default to hash distributed joins if no cost could be computed, such as if
the tables do not have statistics. 

The corresponding configuration property is :ref:`admin/properties:\`\`join-distribution-type\`\``. 


``redistribute_writes``
^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

This property enables redistribution of data before writing. This can
eliminate the performance impact of data skew when writing by hashing it
across nodes in the cluster. It can be disabled when it is known that the
output data set is not skewed in order to avoid the overhead of hashing and
redistributing all the data across the network.

The corresponding configuration property is :ref:`admin/properties:\`\`redistribute-writes\`\``. 

``task_writer_count``
^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``1``

Default number of local parallel table writer threads per worker. It is required
to be a power of two for a Java query engine.

The corresponding configuration property is :ref:`admin/properties:\`\`task.writer-count\`\``. 

``task_partitioned_writer_count``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``task_writer_count``

Number of local parallel table writer threads per worker for partitioned writes. If not
set, the number set by ``task_writer_count`` will be used. It is required to be a power
of two for a Java query engine.

Spilling Properties
-------------------

``spill_enabled``
^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Try spilling memory to disk to avoid exceeding memory limits for the query.

Spilling works by offloading memory to disk. This process can allow a query with a large memory
footprint to pass at the cost of slower execution times. Currently, spilling is supported only for
aggregations and joins (inner and outer), so this property will not reduce memory usage required for
window functions, sorting and other join types.

Be aware that this is an experimental feature and should be used with care.

The corresponding configuration property is :ref:`admin/properties:\`\`experimental.spill-enabled\`\``. 

``join_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for joins to
avoid exceeding memory limits for the query.

The corresponding configuration property is :ref:`admin/properties:\`\`experimental.join-spill-enabled\`\``. 

``aggregation_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for aggregations to
avoid exceeding memory limits for the query.

The corresponding configuration property is :ref:`admin/properties:\`\`experimental.aggregation-spill-enabled\`\``. 

``distinct_aggregation_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``aggregation_spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for distinct
aggregations to avoid exceeding memory limits for the query.

The corresponding configuration property is :ref:`admin/properties:\`\`experimental.distinct-aggregation-spill-enabled\`\``. 

``order_by_aggregation_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``aggregation_spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for order by
aggregations to avoid exceeding memory limits for the query.

The corresponding configuration property is :ref:`admin/properties:\`\`experimental.order-by-aggregation-spill-enabled\`\``. 

``window_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for window functions to
avoid exceeding memory limits for the query.

The corresponding configuration property is :ref:`admin/properties:\`\`experimental.window-spill-enabled\`\``. 

``order_by_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When ``spill_enabled`` is ``true``, this determines whether Presto will try spilling memory to disk for order by to
avoid exceeding memory limits for the query.

The corresponding configuration property is :ref:`admin/properties:\`\`experimental.order-by-spill-enabled\`\``. 

``aggregation_operator_unspill_memory_limit``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``data size``
* **Default value:** ``4 MB``

Limit for memory used for unspilling a single aggregation operator instance.

The corresponding configuration property is :ref:`admin/properties:\`\`experimental.aggregation-operator-unspill-memory-limit\`\``. 

Task Properties
---------------

``task_concurrency``
^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Restrictions:** must be a power of two
* **Default value:** ``16``

Default local concurrency for parallel operators such as joins and aggregations.
This value should be adjusted up or down based on the query concurrency and worker
resource utilization. Lower values are better for clusters that run many queries
concurrently because the cluster will already be utilized by all the running
queries, so adding more concurrency will result in slow downs due to context
switching and other overhead. Higher values are better for clusters that only run
one or a few queries at a time. 

The corresponding configuration property is :ref:`admin/properties:\`\`task.concurrency\`\``. 

``task_writer_count``
^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Restrictions:** must be a power of two
* **Default value:** ``1``

The number of concurrent writer threads per worker per query. Increasing this value may
increase write speed, especially when a query is not I/O bound and can take advantage
of additional CPU for parallel writes (some connectors can be bottlenecked on CPU when
writing due to compression or other factors). Setting this too high may cause the cluster
to become overloaded due to excessive resource utilization. 

The corresponding configuration property is :ref:`admin/properties:\`\`task.writer-count\`\``. 

Optimizer Properties
--------------------

``dictionary_aggregation``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Enables optimization for aggregations on dictionaries. 

The corresponding configuration property is :ref:`admin/properties:\`\`optimizer.dictionary-aggregation\`\``. 

``optimize_hash_generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Compute hash codes for distribution, joins, and aggregations early during execution,
allowing result to be shared between operations later in the query. This can reduce
CPU usage by avoiding computing the same hash multiple times, but at the cost of
additional network transfer for the hashes. In most cases it will decrease overall
query processing time. 

It is often helpful to disable this property when using :doc:`/sql/explain` in order
to make the query plan easier to read.

The corresponding configuration property is :ref:`admin/properties:\`\`optimizer.optimize-hash-generation\`\``. 

``push_aggregation_through_join``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

When an aggregation is above an outer join and all columns from the outer side of the join
are in the grouping clause, the aggregation is pushed below the outer join. This optimization
is particularly useful for correlated scalar subqueries, which get rewritten to an aggregation
over an outer join. For example::

    SELECT * FROM item i
        WHERE i.i_current_price > (
            SELECT AVG(j.i_current_price) FROM item j
                WHERE i.i_category = j.i_category);

Enabling this optimization can substantially speed up queries by reducing
the amount of data that needs to be processed by the join.  However, it may slow down some
queries that have very selective joins. 

The corresponding configuration property is :ref:`admin/properties:\`\`optimizer.push-aggregation-through-join\`\``. 

``push_table_write_through_union``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

Parallelize writes when using ``UNION ALL`` in queries that write data. This improves the
speed of writing output tables in ``UNION ALL`` queries because these writes do not require
additional synchronization when collecting results. Enabling this optimization can improve
``UNION ALL`` speed when write speed is not yet saturated. However, it may slow down queries
in an already heavily loaded system. 

The corresponding configuration property is :ref:`admin/properties:\`\`optimizer.push-table-write-through-union\`\``. 

``join_reordering_strategy``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Allowed values:** ``AUTOMATIC``, ``ELIMINATE_CROSS_JOINS``, ``NONE``
* **Default value:** ``AUTOMATIC``

The join reordering strategy to use.  ``NONE`` maintains the order the tables are listed in the
query.  ``ELIMINATE_CROSS_JOINS`` reorders joins to eliminate cross joins where possible and
otherwise maintains the original query order. When reordering joins it also strives to maintain the
original table order as much as possible. ``AUTOMATIC`` enumerates possible orders and uses
statistics-based cost estimation to determine the least cost order. If stats are not available or if
for any reason a cost could not be computed, the ``ELIMINATE_CROSS_JOINS`` strategy is used. 

The corresponding configuration property is :ref:`admin/properties:\`\`optimizer.join-reordering-strategy\`\``. 

``confidence_based_broadcast``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Enable broadcasting based on the confidence of the statistics that are being used, by
broadcasting the side of a joinNode which has the highest (``HIGH`` or ``FACT``) confidence statistics.
If both sides have the same confidence statistics, then the original behavior will be followed.

The corresponding configuration property is :ref:`admin/properties:\`\`optimizer.confidence-based-broadcast\`\``. 

``treat-low-confidence-zero-estimation-as-unknown``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Enable treating ``LOW`` confidence, zero estimations as ``UNKNOWN`` during joins. 

The corresponding configuration property is :ref:`admin/properties:\`\`optimizer.treat-low-confidence-zero-estimation-as-unknown\`\``. 

``retry-query-with-history-based-optimization``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Enable retry for failed queries who can potentially be helped by HBO. 

The corresponding configuration property is :ref:`admin/properties:\`\`optimizer.retry-query-with-history-based-optimization\`\``. 
