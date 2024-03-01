=======
ANALYZE
=======

Synopsis
--------

.. code-block:: none

    ANALYZE table_name [ WITH ( property_name = expression [, ...] ) ]

Description
-----------

Collects table and column statistics for a given table.
Currently column statistics are only collected for primitive types.

The optional ``WITH`` clause can be used to provide
connector-specific properties. To list all available properties, run the following query::

    SELECT * FROM system.metadata.analyze_properties

Currently, this statement is only supported by the
:ref:`Hive connector <hive_analyze>`.

Examples
--------

Analyze table ``web`` to collect table and column statistics::

    ANALYZE web;

Analyze table ``stores`` in catalog ``hive`` and schema ``default``::

    ANALYZE hive.default.stores;

Analyze partitions ``'1992-01-01', '1992-01-02'`` from a Hive partitioned table ``sales``::

    ANALYZE hive.default.sales WITH (partitions = ARRAY[ARRAY['1992-01-01'], ARRAY['1992-01-02']]);

Analyze partitions with complex partition key (``state`` and ``city`` columns) from a Hive partitioned table ``customers``::

    ANALYZE hive.default.customers WITH (partitions = ARRAY[ARRAY['CA', 'San Francisco'], ARRAY['NY', 'NY']]);

