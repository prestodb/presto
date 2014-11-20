============
Release 0.93
============

Verifier
--------

* Add support for setting username and password per query

If you're upgrading from 0.92, you need to alter your verifier_queries table

.. code-block:: sql

    ALTER TABLE verifier_queries add test_username VARCHAR(256) NOT NULL default 'verifier-test';
    ALTER TABLE verifier_queries add test_password VARCHAR(256);
    ALTER TABLE verifier_queries add control_username VARCHAR(256) NOT NULL default 'verifier-test';
    ALTER TABLE verifier_queries add control_password VARCHAR(256);

