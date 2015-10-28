=============
Release 0.124
=============

General Changes
---------------

* Fix race in memory tracking of ``JOIN`` which could cause the cluster to become over
  committed and possibly crash.
* The :func:`approx_percentile` aggregation now also accepts an array of percentages.

Hive Changes
------------

* Do not count expected exceptions as errors in the Hive metastore client stats.
* Improve performance when reading ORC files with many tiny stripes.

Verifier
--------

* Add support for pre and post control and test queries.

If you are upgrading, you need to alter your ``verifier_queries`` table::

    ALTER TABLE verifier_queries ADD COLUMN test_postqueries text;
    ALTER TABLE verifier_queries ADD COLUMN test_prequeries text;
    ALTER TABLE verifier_queries ADD COLUMN control_postqueries text;
    ALTER TABLE verifier_queries ADD COLUMN control_prequeries text;
