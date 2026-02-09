=====================
JMX Metrics Reference
=====================

Presto exposes comprehensive metrics via Java Management Extensions (JMX) for monitoring
cluster health, query performance, and system behavior. This page documents some
important JMX metrics available for production monitoring.

Overview
--------

JMX metrics can be accessed through:

* **JMX clients**: JConsole, VisualVM, or jmxterm
* **SQL queries**: Using the :doc:`/connector/jmx` connector
* **Monitoring systems**: Prometheus, Grafana, or other JMX exporters

Querying Metrics via SQL
-------------------------

Once configured, you can query metrics using SQL:

.. code-block:: sql

    -- List all available metrics
    SHOW TABLES FROM jmx.current;

    -- Query specific metrics
    SELECT * FROM jmx.current."com.facebook.presto.metadata:name=metadatamanagerstats";

Metadata Operation Metrics
---------------------------

**JMX Table Name:** ``com.facebook.presto.metadata:name=metadatamanagerstats``

Tracks performance and usage of all metadata operations including schema discovery,
table lookups, and column information retrieval.

Key Metrics
^^^^^^^^^^^

For each metadata operation such as ``listSchemaNames``, ``listTables``, or ``getTableHandle``:

**Call Counters**

* ``<operation>Calls``: Total number of times the operation was called
* Example: ``listSchemaNamesCalls``, ``listTablesCalls``

**Timing Statistics**

All timing values are in nanoseconds:

* ``<operation>time.alltime.avg``: Average execution time across all calls
* ``<operation>time.alltime.min``: Fastest execution time
* ``<operation>time.alltime.max``: Slowest execution time
* ``<operation>time.alltime.count``: Number of samples collected
* ``<operation>time.alltime.p50``: Median (50th percentile)
* ``<operation>time.alltime.p75``: 75th percentile
* ``<operation>time.alltime.p90``: 90th percentile
* ``<operation>time.alltime.p95``: 95th percentile
* ``<operation>time.alltime.p99``: 99th percentile

**Time Windows**

Statistics are also available for recent time windows:

* ``<operation>time.oneminute.*``: Last 1 minute
* ``<operation>time.fiveminutes.*``: Last 5 minutes
* ``<operation>time.fifteenminutes.*``: Last 15 minutes

Common Operations
^^^^^^^^^^^^^^^^^

**Schema Operations**

* ``listSchemaNames``: List all schemas in a catalog
* ``getSchemaProperties``: Get schema-level properties

**Table Operations**

* ``listTables``: List tables in a schema
* ``getTableHandle``: Get table metadata handle
* ``getTableMetadata``: Get detailed table information
* ``getTableStatistics``: Get table statistics

**Column Operations**

* ``getColumnHandles``: Get column information
* ``getColumnMetadata``: Get detailed column metadata

**View Operations**

* ``listViews``: List views in a schema
* ``getView``: Get view definition

Example Queries
^^^^^^^^^^^^^^^

**Query Lifecycle Metrics**

Track query begin and completion times:

.. code-block:: sql

    -- Query begin operation metrics
    SELECT
        "beginquerytime.alltime.count" as total_queries,
        "beginquerytime.alltime.avg" / 1000.0 as avg_microseconds,
        "beginquerytime.alltime.min" / 1000.0 as min_microseconds,
        "beginquerytime.alltime.max" / 1000.0 as max_microseconds
    FROM jmx.current."com.facebook.presto.metadata:name=metadatamanagerstats";

    -- Example output:
    -- total_queries | avg_microseconds | min_microseconds | max_microseconds
    -- 3.0           | 49.42            | 28.63            | 75.38

**Insert Operation Metrics**

Track data insertion performance:

.. code-block:: sql

    -- Begin insert operation metrics
    SELECT
        "begininserttime.alltime.count" as insert_operations,
        "begininserttime.alltime.avg" / 1000000000.0 as avg_seconds,
        "begininserttime.alltime.min" / 1000000000.0 as min_seconds,
        "begininserttime.alltime.max" / 1000000000.0 as max_seconds
    FROM jmx.current."com.facebook.presto.metadata:name=metadatamanagerstats";

    -- Example output:
    -- insert_operations | avg_seconds | min_seconds | max_seconds
    -- 1.0               | 0.82        | 0.82        | 0.82

    -- Finish insert operation metrics
    SELECT
        "finishinserttime.alltime.count" as completed_inserts,
        "finishinserttime.alltime.avg" / 1000000000.0 as avg_seconds,
        "finishinserttime.alltime.min" / 1000000000.0 as min_seconds,
        "finishinserttime.alltime.max" / 1000000000.0 as max_seconds
    FROM jmx.current."com.facebook.presto.metadata:name=metadatamanagerstats";

    -- Example output:
    -- completed_inserts | avg_seconds | min_seconds | max_seconds
    -- 1.0               | 11.47       | 11.47       | 11.47

System Access Control Metrics
------------------------------

**JMX Table Name:** ``com.facebook.presto.security:name=accesscontrolmanager``

Tracks performance of access control checks.

Key Metrics
^^^^^^^^^^^

Similar structure to metadata metrics, tracking operations like:

* ``checkCanSetUser``: User impersonation checks
* ``checkCanAccessCatalog``: Catalog access checks
* ``checkCanSelectFromColumns``: Column-level access checks
* ``checkCanCreateTable``: Table creation permission checks

Query Execution Metrics
-----------------------

**Task Metrics**

* ``com.facebook.presto.execution:name=taskmanager``: Task execution statistics
* ``com.facebook.presto.execution.executor:name=taskexecutor``: Task executor pool metrics

**Memory Metrics**

* ``com.facebook.presto.memory:name=general,type=memorypool``: General memory pool usage
* ``com.facebook.presto.memory:name=reserved,type=memorypool``: Reserved memory pool usage

**Query Manager Metrics**

* ``com.facebook.presto.dispatcher:name=dispatchmanager``: Query dispatch statistics
* ``com.facebook.presto.execution:name=querymanager``: Query execution statistics

Connector-Specific Metrics
---------------------------

Hive Connector
^^^^^^^^^^^^^^

* ``com.facebook.presto.hive:name=*``: Hive metastore and file system metrics
Example -
* com.facebook.presto.hive:name=hive,type=cachingdirectorylister

Iceberg Connector
^^^^^^^^^^^^^^^^^

* ``com.facebook.presto.iceberg:name=*``: Iceberg-specific caching and I/O metrics

Examples:

* com.facebook.presto.iceberg:name=iceberg,type=icebergsplitmanager
* com.facebook.presto.iceberg:name=iceberg,type=manifestfilecache
* com.facebook.presto.iceberg:name=icebergfilewriterfactory

See Also
--------

* :doc:`/connector/jmx` - JMX Connector documentation
* :doc:`web-interface` - Web UI monitoring
* :doc:`tuning` - Performance tuning guide
