============
Release 0.60
============

JDBC improvements
-----------------

The Presto version of the JDBC ``DatabaseMetaData`` interface now includes
proper implementations of ``getTables``, ``getSchemas`` and ``getCatalogs``.

The JDBC driver is now always packaged as a standalone jar without any
dependencies.  Previously, this artifact was published with the Maven
classifier ``standalone``. The new build does not publish this artifact
anymore.

USE CATALOG and USE SCHEMA
--------------------------

The :doc:`/installation/cli` now supports ``USE CATALOG`` and
``USE SCHEMA``.


TPCH Connector
--------------

We have added a new connector that will generate synthetic data following the
TPC-H specification. This connector makes it easy to generate large datasets for
testing and bug reports. When generating bug reports, we encourage users to use
this catalog since it eases the process of reproducing the issue. The data is
generated dynamically for each query, so no disk space is used by this
connector. To add the ``tpch`` catalog to your system, create the catalog
property file ``etc/catalog/tpch.properties`` on both the coordinator and workers
with the following contents:

.. code-block:: none

    connector.name=tpch

Additionally, update the ``datasources`` property in the config properties file,
``etc/config.properties``, for the workers to include ``tpch``.

SPI changes
-----------

The ``Connector`` interface now has explicit methods for supplying the services
expected by the query engine. Previously, this was handled by a generic
``getService`` method.

.. note::
    This is a backwards incompatible change to ``Connector`` in the SPI,
    so if you have written a connector, you will need to update your code before
    deploying this release.

Additionally, we have added the ``NodeManager`` interface to the SPI to allow a
plugin to detect all nodes in the Presto cluster.  This is important for some
connectors that can divide a table evenly between all nodes as long as the
connector knows how many nodes exist.  To access the node manager, simply add
the following to the ``Plugin`` class:

.. code-block:: java

    @Inject
    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

Optimizations
-------------

DISTINCT LIMIT
~~~~~~~~~~~~~~

For queries with the following form::

    SELECT DISTINCT ...
    FROM T
    LIMIT N

We have added an optimization that stops the query as soon as ``N`` distinct
rows are found.

Range predicates
~~~~~~~~~~~~~~~~

When optimizing a join, Presto analyzes the ranges of the partitions on each
side of a join and pushes these ranges to the other side.  When tables have a
lot of partitions, this can result in a very large filter with one expression
for each partition.  The optimizer now summarizes the predicate ranges to reduce
the complexity of the filters.

Compound filters
~~~~~~~~~~~~~~~~

Complex expressions involving ``AND``, ``OR``, or ``NOT`` are now optimized by
the expression optimizer.

Window functions
~~~~~~~~~~~~~~~~

Window functions with a ``PARTITION BY`` clause are now distributed based on the
partition key.

Bug fixes
---------

* Scheduling

  In the changes to schedule splits in batches, we introduced two bugs that
  resulted in an unbalanced workload across nodes which increases query latency.
  The first problem was not inspecting the queued split count of the nodes while
  scheduling the batch, and the second problem was not counting the splits
  awaiting creation in the task executor.

* JSON conversion of complex Hive types

  Presto converts complex Hive types (array, map, struct and union) into JSON.
  Previously, numeric keys in maps were converted to numbers, not strings,
  which is invalid as JSON only allows strings for object keys. This prevented
  the :doc:`/functions/json` from working.

* Hive hidden files

  Presto will now ignore files in Hive that start with an underscore ``_`` or
  a dot ``.``.  This matches the behavior of Hadoop MapReduce / Hive.

* Failures incorrectly reported as no data

  Certain types of failures would result in the query appearing to succeed and
  return an incomplete result (often zero rows). There was a race condition
  between the error propagation and query teardown. In some cases, the query
  would be torn down before the exception made it to the coordinator. This was a
  regression introduced during the query teardown optimization work. There are
  now tests to catch this type of bug.

* Exchange client leak

  When a query finished early (e.g., limit or failure) and the exchange operator
  was blocked waiting for data from other nodes, the exchange was not be closed
  properly. This resulted in continuous failing HTTP requests which leaked
  resources and produced large log files.

* Hash partitioning

  A query with many ``GROUP BY`` items could fail due to an overflow in the hash
  function.

* Compiled NULL literal

  In some cases queries with a select expression like ``CAST(NULL AS varchar)``
  would fail due to a bug in the output type detection code in expression
  compiler.
