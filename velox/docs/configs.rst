========================
Configuration properties
========================

Memory Management
-----------------

``max_partial_aggregation_memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``16MB``

Maximum amount of memory in bytes for partial aggregation results. Increasing
this value can result in less network transfer and lower CPU utilization by
allowing more groups to be kept locally before being flushed, at the cost of
additional memory usage.

``max_extended_partial_aggregation_memory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``16MB``

Maximum amount of memory in bytes for partial aggregation results if cardinality
reduction is below `partial_aggregation_reduction_ratio_threshold`. Every time partial
aggregate results size reaches `max_partial_aggregation_memory` bytes, the results
are flushed. If cardinality reduction is below `partial_aggregation_reduction_ratio_threshold`,
i.e. `number of result rows / number of input rows > partial_aggregation_reduction_ratio_threshold`,
memory limit for partial aggregation is automatically doubled up to
`max_extended_partial_aggregation_memory`. This adaptation is disabled by default, since
the value of `max_extended_partial_aggregation_memory` equals the value of
`max_partial_aggregation_memory`. Specify higher value for `max_extended_partial_aggregation_memory`
to enable.

``partial_aggregation_reduction_ratio_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``double``
    * **Default value:** ``0.5``

Cardinality reduction threshold for partial aggregation to enable adaptive memory limit increase
up to `max_extended_partial_aggregation_memory`. Valid values are between 0 and 1. If
partial aggregation results reach `max_partial_aggregation_memory` limit and
`number of result rows / number of input rows > partial_aggregation_reduction_ratio_threshold`
the limit is automatically doubled up to `max_extended_partial_aggregation_memory`.

Spilling
--------

``spill_enabled``
^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

Spill memory to disk to avoid exceeding memory limits for the query.

``spiller-spill-path``
^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``string``
    * **Default value:** ``/tmp``

Directory where spilled content is written.

``aggregation_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

When `spill_enabled` is true, determines whether to spill memory to disk
for aggregations to avoid exceeding memory limits for the query.

``join_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

When `spill_enabled` is true, determines whether to spill memory to disk
for hash joins to avoid exceeding memory limits for the query.

``order_by_spill_enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``boolean``
    * **Default value:** ``false``

When `spill_enabled` is true, determines whether to spill memory to disk
for order by to avoid exceeding memory limits for the query.

``aggregation_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``0``

Maximum amount of memory in bytes that a final aggregation can use before spilling.
0 means unlimited.

``join_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``0``

Maximum amount of memory in bytes that a hash join build side can use before spilling.
0 means unlimited.

``order_by_spill_memory_threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``0``

Maximum amount of memory in bytes that an order by can use before spilling.
0 means unlimited.

``max-spill-level``
^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``4``

The maximum allowed spilling level with zero being the initial spilling level.
Applies to hash join build spilling which might use recursive spilling when
the build table is very large. -1 means unlimited. In this case an extremely
large query might run out of spilling partition bits. The max spill level
can be used to prevent a query from using too much io and cpu resources.

``max-spill-file-size``
^^^^^^^^^^^^^^^^^^^^^^^

    * **Type:** ``integer``
    * **Default value:** ``0``

The maximum allowed spill file size. Zero means unlimited.
