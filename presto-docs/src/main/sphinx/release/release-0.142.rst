=============
Release 0.142
=============

General Changes
---------------

* Fix planning bug for ``JOIN`` criteria that optimizes to a ``FALSE`` expression.
* Fix planning bug when the output of ``UNION`` doesn't match the table column order
  in ``INSERT`` queries.
* Fix error when ``ORDER BY`` clause in window specification refers to the same column multiple times.
* Add support for :ref:`complex grouping operations<complex_grouping_operations>`
  - ``CUBE``, ``ROLLUP`` and ``GROUPING SETS``.
* Add support for ``IF NOT EXISTS`` in ``CREATE TABLE AS`` queries.
* Add :func:`substring` function.
* Add ``http.server.authentication.krb5.keytab`` config option to set the location of the Kerberos
  keytab file explicitly.
* Add ``optimize_metadata_queries`` session property to enable the metadata-only query optimization.
* Improve support for non-equality predicates in ``JOIN`` criteria.
* Add support for non-correlated subqueries in aggregation queries.
* Improve performance of :func:`json_extract`.

Hive Changes
------------

* Change ORC input format to report actual bytes read as opposed to estimated bytes.
* Fix cache invalidation when renaming tables.
* Fix Parquet reader to handle uppercase column names.
* Fix issue where the ``hive.respect-table-format`` config option was being ignored.
* Add :doc:`hive.compression-codec </connector/hive>` config option to control
  compression used when writing. The default is now ``GZIP`` for all formats.
* Collect and expose end-to-end execution time JMX metric for requests to AWS services.
