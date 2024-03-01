=============
Release 0.166
=============

General Changes
---------------

* Fix failure due to implicit coercion issue in ``IN`` expressions for
  certain combinations of data types (e.g., ``double`` and ``decimal``).
* Add ``query.max-length`` config flag to set the maximum length of a SQL query.
  The default maximum length is 1MB.
* Improve performance of :func:`approx_percentile`.

Hive Changes
------------

* Include original exception from metastore for ``AlreadyExistsException`` when adding partitions.
* Add support for the Hive JSON file format (``org.apache.hive.hcatalog.data.JsonSerDe``).

Cassandra Changes
-----------------

* Add configuration properties for speculative execution.

SPI Changes
-----------

* Add peak memory reservation to ``SplitStatistics`` in split completion events.
