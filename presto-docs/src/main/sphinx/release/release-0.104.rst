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

Hive Changes
------------

* Upgrade to Parquet 1.6.0.
* Collect request time in ``PrestoS3FileSystem``.
