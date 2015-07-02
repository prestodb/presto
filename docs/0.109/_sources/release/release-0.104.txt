=============
Release 0.104
=============

General Changes
---------------

* Handle thread interruption in StatementClient.
* Fix CLI hang when server becomes unreachable during a query.
* Add :func:`covar_pop`, :func:`covar_samp`, :func:`corr`, :func:`regr_slope`,
  and :func:`regr_intercept` functions.
* Fix potential deadlock in cluster memory manager.
* Add a visualization of query execution timeline.
* Allow mixed case in input to :func:`from_hex`.
* Display "BLOCKED" state in web UI.
* Reduce CPU usage in coordinator.
* Fix excess object retention in workers due to long running queries.
* Reduce memory usage of :func:`array_distinct`.
* Add optimizer for projection push down which can
  improve the performance of certain query shapes.
* Improve query performance by storing pre-partitioned pages.
* Support ``TIMESTAMP`` for :func:`first_value`, :func:`last_value`,
  :func:`nth_value`, :func:`lead` and :func:`lag`.

Hive Changes
------------

* Upgrade to Parquet 1.6.0.
* Collect request time and retry statistics in ``PrestoS3FileSystem``.
* Fix retry attempt counting for S3.
