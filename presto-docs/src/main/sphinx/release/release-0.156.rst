=============
Release 0.156
=============

General Changes
---------------

* Fix potential correctness issue in queries that contain correlated scalar aggregation subqueries.
* Fix query failure when using ``AT TIME ZONE`` in ``VALUES`` list.
* Add support for quantified comparison predicates: ``ALL``, ``ANY``, and ``SOME``.
* Add support for :ref:`array_type` and :ref:`row_type` that contain ``NULL``
  in :func:`checksum` aggregation.
* Add support for filtered aggregations. Example: ``SELECT sum(a) FILTER (WHERE b > 0) FROM ...``
* Add a variant of :func:`from_unixtime` function that takes a timezone argument.
* Improve performance of ``GROUP BY`` queries that compute a mix of distinct
  and non-distinct aggregations. This optimization can be turned on by setting
  the ``optimizer.optimize-mixed-distinct-aggregations`` configuration option or
  via the ``optimize_mixed_distinct_aggregations`` session property.
* Change default task concurrency to 16.

Hive Changes
------------

* Add support for legacy RCFile header version in new RCFile reader.

Redis Changes
-------------

* Support ``iso8601`` data format for the ``hash`` row decoder.

SPI Changes
-----------

* Make ``ConnectorPageSink#finish()`` asynchronous.

.. note::
    These are backwards incompatible changes with the previous SPI.
    If you have written a plugin, you will need to update your code
    before deploying this release.
