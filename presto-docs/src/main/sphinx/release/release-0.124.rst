=============
Release 0.124
=============

General Changes
---------------

* The :func:`approx_percentile` aggregation now also accepts an array of percentages

Verifier
--------

* Add support for pre and post control and test queries

If you're upgrading from 0.123, you need to alter your verifier_queries table

.. code-block:: sql

    ALTER TABLE verifier_queries add column test_postqueries text null;
    ALTER TABLE verifier_queries add column test_prequeries text null;
    ALTER TABLE verifier_queries add column control_postqueries text null;
    ALTER TABLE verifier_queries add column control_prequeries text null;