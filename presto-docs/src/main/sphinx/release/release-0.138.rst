=============
Release 0.138
=============

General Changes
---------------

* Fix planning bug with ``NULL`` literal coercions.
* Reduce query startup time by reducing lock contention in scheduler.

New Hive Parquet Reader
-----------------------

We have added a new Parquet reader implementation. The new reader supports vectorized
reads, lazy loading, and predicate push down, all of which make the reader more
efficient and typically reduces wall clock time for a query. Although the new
reader has been heavily tested, it is an extensive rewrite of the Apache Hive
Parquet reader, and may have some latent issues, so it is not enabled by default.
If you are using Parquet we suggest you test out the new reader on a per-query basis
by setting the ``<hive-catalog>.parquet_optimized_reader_enabled`` session property,
or you can enable the reader by default by setting the Hive catalog property
``hive.parquet-optimized-reader.enabled=true``.  To enable Parquet predicate push down
there is a separate session property ``<hive-catalog>.parquet_predicate_pushdown_enabled``
and configuration property ``hive.parquet-predicate-pushdown.enabled=true``.
