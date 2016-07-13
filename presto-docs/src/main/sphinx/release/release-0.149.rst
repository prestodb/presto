=============
Release 0.149
=============

General Changes
---------------

* Fix runtime failure for queries that use grouping sets over unions.
* Do not ignore null values in :func:`array_agg`.
* Fix failure when casting row values that contain null fields.
* Fix failure when using complex types as map keys.
* Fix potential memory tracking leak when queries are cancelled.
* Fix rejection of queries that do not match any queue/resource group rules.
  Previously, a 500 error was returned to the client.
* Fix :func:`trim` and :func:`rtrim` functions to produce more intuitive results
  when the argument contains invalid ``UTF-8`` sequences.
* Add a new web interface with cluster overview, realtime stats, and improved sorting
  and filtering of queries.
* Add support for ``FLOAT`` type.
* Rename ``query.max-age`` to ``query.min-expire-age``.
* ``optimizer.columnar-processing`` and ``optimizer.columnar-processing-dictionary``
  properties were merged to ``optimizer.processing-optimization`` with possible
  values ``disabled``, ``columnar`` and ``columnar_dictionary``
* ``columnar_processing`` and ``columnar_processing_dictionary`` session
  properties were merged to ``processing_optimization`` with possible values
  ``disabled``, ``columnar`` and ``columnar_dictionary``
* Change ``%y`` (2-digit year) in :func:`date_parse` to evaluate to a year between
  1970 and 2069 inclusive.
* Add ``queued`` flag to ``StatementStats`` in REST API.
* Improve error messages for math operations.
* Improve memory tracking in exchanges to avoid running out of Java heap space.
* Improve performance of subscript operator for the ``MAP`` type.
* Improve performance of ``JOIN`` and ``GROUP BY`` queries.

Hive Changes
------------

* Clean up empty staging directories after inserts.
* Add ``hive.dfs.ipc-ping-interval`` config for HDFS.
* Change default value of ``hive.dfs-timeout`` to 60 seconds.
* Fix ORC/DWRF reader to avoid repeatedly fetching the same data when stripes
  are skipped.
* Fix force local scheduling for S3 or other non-HDFS file systems.
