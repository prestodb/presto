============
Release 0.93
============

ORC Memory Usage
----------------

This release changes the Presto ORC reader to favor small buffers when reading
varchar and varbinary data. Some ORC files contain columns of data that are
hundreds of megabytes when decompressed. In the previous Presto ORC reader, we
would allocate a single large shared buffer for all values in the column. This
would cause heap fragmentation in CMS and G1, and it would cause OOMs since
each value of the column retains a reference to the shared buffer. In this
release the ORC reader uses a separate buffer for each value in the column.
This reduces heap fragmentation and excessive memory retention at the expense
of object creation.

Verifier
--------

* Add support for setting username and password per query

If you're upgrading from 0.92, you need to alter your verifier_queries table

.. code-block:: sql

    ALTER TABLE verifier_queries add test_username VARCHAR(256) NOT NULL default 'verifier-test';
    ALTER TABLE verifier_queries add test_password VARCHAR(256);
    ALTER TABLE verifier_queries add control_username VARCHAR(256) NOT NULL default 'verifier-test';
    ALTER TABLE verifier_queries add control_password VARCHAR(256);

General Changes
---------------

* Add optimizer for ``LIMIT 0``
* Fix incorrect check to disable string statistics in ORC
* Ignore hidden columns in ``INSERT`` and ``CREATE TABLE AS`` queries
* Add SOCKS support to CLI
* Improve CLI output for update queries
* Disable pushdown for non-deterministic predicates
