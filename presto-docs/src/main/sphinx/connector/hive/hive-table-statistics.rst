===============================
Hive Connector Table Statistics
===============================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Table Statistics
----------------

The Hive connector automatically collects basic statistics
(``numFiles``, ``numRows``, ``rawDataSize``, ``totalSize``)
on ``INSERT`` and ``CREATE TABLE AS`` operations.

The Hive connector can also collect column level statistics:

============= ====================================================================
Column Type   Collectible Statistics
============= ====================================================================
``TINYINT``   number of nulls, number of distinct values, min/max values
``SMALLINT``  number of nulls, number of distinct values, min/max values
``INTEGER``   number of nulls, number of distinct values, min/max values
``BIGINT``    number of nulls, number of distinct values, min/max values
``DOUBLE``    number of nulls, number of distinct values, min/max values
``REAL``      number of nulls, number of distinct values, min/max values
``DECIMAL``   number of nulls, number of distinct values, min/max values
``DATE``      number of nulls, number of distinct values, min/max values
``TIMESTAMP`` number of nulls, number of distinct values, min/max values
``VARCHAR``   number of nulls, number of distinct values
``CHAR``      number of nulls, number of distinct values
``VARBINARY`` number of nulls
``BOOLEAN``   number of nulls, number of true/false values
============= ====================================================================

Automatic column level statistics collection on write is controlled by
the ``collect_column_statistics_on_write`` catalog session property.

.. _hive_analyze:

Collecting table and column statistics
--------------------------------------

The Hive connector supports collection of table and partition statistics
using the :doc:`/sql/analyze` statement. When analyzing a partitioned table,
the partitions to analyze can be specified via the optional ``partitions``
property, which is an array containing the values of the partition keys
in the order they are declared in the table schema::

    ANALYZE hive.sales WITH (
        partitions = ARRAY[
            ARRAY['partition1_value1', 'partition1_value2'],
            ARRAY['partition2_value1', 'partition2_value2']]);

This query will collect statistics for 2 partitions with keys:

* ``partition1_value1, partition1_value2``
* ``partition2_value1, partition2_value2``

Quick Stats
-----------

The Hive connector can build basic statistics for partitions with missing statistics
by examining file or table metadata. For example, Parquet footers can be used to infer
row counts, number of nulls, and min/max values. These quick statistics help in query planning
and serve as as a temporary source of stats for partitions which haven't had ANALYZE run on
them.

The following properties can be used to control how these quick stats are built:

.. list-table::
   :widths: 20 70 10
   :header-rows: 1

   -

      - Property Name
      - Description
      - Default
   -

      - ``hive.quick-stats.enabled``
      - Enable stats collection through quick stats providers. Also
        toggleable through the ``quick_stats_enabled`` session property.
      - ``false``
   -

      - ``hive.quick-stats.max-concurrent-calls``
      - Quick stats are built for multiple partitions concurrently. This
        property sets the maximum number of concurrent builds that can
        be made.
      - 100
   -

      - ``hive.quick-stats.inline-build-timeout``
      - Duration the query that initiates a quick stats build for a
        partition should wait before timing out and returning empty
        stats. Set this to ``0s`` if you want quick stats to only be
        built in the background and not block query planning.
        Also toggleable through the ``quick_stats_inline_build_timeout``
        session property.
      - ``60s``
   -

      - ``hive.quick-stats.background-build-timeout``
      - If a query observes that quick stats are being built for
        a partition by another query, this is the duration it waits for
        those stats to be built before returning empty stats.
        Set this to ``0s`` if you want only one query to wait for
        quick stats to be built (for a given partition).
      - ``0s``
   -

      - ``hive.quick-stats.cache-expiry``
      - Duration to retain the stats in the quick stats in-memory cache.
      - ``24h``
   -

      - ``hive.quick-stats.reaper-expiry``
      - If the quick stats build for a partition is stuck (for example, due to
        a long-running IO operation), a reaper job terminates any background
        build threads so that a new fetch could be triggered afresh.
        This property controls the duration, after a background build
        thread is started, for the reaper to perform the termination.
      - ``5m``
   -

      - ``hive.quick-stats.parquet.max-concurrent-calls``
      - Multiple Parquet file footers are read and processed
        concurrently. This property sets the maximum number of
        concurrent calls that can be made.
      - 500
   -

      - ``hive.quick-stats.parquet.file-metadata-fetch-timeout``
      - Duration after which the Parquet quick stats builder will fail
        and return empty stats.
      - ``60s``