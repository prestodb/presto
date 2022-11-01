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
