=============
Release 0.124
=============

General Changes
---------------

* The :func:`approx_percentile` aggregation now also accepts an array of percentages.

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
