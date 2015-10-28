=============
Release 0.124
=============

General Changes
---------------

* The :func:`approx_percentile` aggregation now also accepts an array of percentages.
* Fix issue where invalid plans are generated for queries with multiple aggregations that require input values to be cast in different ways.
* Fix issue with planner that generated redundant plan nodes for queries that contain both ``DISTINCT`` and ``LIMIT``.

Hive Changes
------------

* Do not count expected exceptions as errors in the Hive metastore client stats.

Verifier
--------

* Add support for pre and post control and test queries.

If you are upgrading, you need to alter your ``verifier_queries`` table::

    ALTER TABLE verifier_queries ADD COLUMN test_postqueries text;
    ALTER TABLE verifier_queries ADD COLUMN test_prequeries text;
    ALTER TABLE verifier_queries ADD COLUMN control_postqueries text;
    ALTER TABLE verifier_queries ADD COLUMN control_prequeries text;
