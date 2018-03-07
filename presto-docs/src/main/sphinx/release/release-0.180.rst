=============
Release 0.180
=============

General Changes
---------------

* Fix a rare bug where rows containing only ``null`` values are not returned
  to the client. This only occurs when an entire result page contains only
  ``null`` values. The only known case is a query over an ORC encoded Hive table
  that does not perform any transformation of the data.
* Fix incorrect results when performing comparisons between values of approximate
  data types (``REAL``, ``DOUBLE``) and columns of certain exact numeric types
  (``INTEGER``, ``BIGINT``, ``DECIMAL``).
* Fix memory accounting for :func:`min_by` and :func:`max_by` on complex types.
* Fix query failure due to ``NoClassDefFoundError`` when scalar functions declared
  in plugins are implemented with instance methods.
* Improve performance of map subscript from O(n) to O(1) in all cases. Previously, only maps
  produced by certain functions and readers could take advantage of this improvement.
* Skip unknown costs in ``EXPLAIN`` output.
* Support :doc:`/security/internal-communication` between Presto nodes.
* Add initial support for ``CROSS JOIN`` against ``LATERAL`` derived tables.
* Add support for ``VARBINARY`` concatenation.
* Add :doc:`/connector/thrift` that makes it possible to use Presto with
  external systems without the need to implement a custom connector.
* Add experimental ``/v1/resourceGroupState`` REST endpoint on coordinator.

Hive Changes
------------

* Fix skipping short decimal values in the optimized Parquet reader
  when they are backed by the ``int32`` or ``int64`` types.
* Ignore partition bucketing if table is not bucketed. This allows dropping
  the bucketing from table metadata but leaving it for old partitions.
* Improve error message for Hive partitions dropped during execution.
* The optimized RCFile writer is enabled by default, but can be disabled
  with the ``hive.rcfile-optimized-writer.enabled`` config option.
  The writer supports validation which reads back the entire file after
  writing. Validation is disabled by default, but can be enabled with the
  ``hive.rcfile.writer.validate`` config option.

Cassandra Changes
-----------------

* Add support for ``INSERT``.
* Add support for pushdown of non-equality predicates on clustering keys.

JDBC Driver Changes
-------------------

* Add support for authenticating using Kerberos.
* Allow configuring SSL/TLS and Kerberos properties on a per-connection basis.
* Add support for executing queries using a SOCKS or HTTP proxy.

CLI Changes
-----------

* Add support for executing queries using an HTTP proxy.

SPI Changes
-----------

* Add running time limit and queued time limit to ``ResourceGroupInfo``.
