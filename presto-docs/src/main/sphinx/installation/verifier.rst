===============
Presto Verifier
===============

The Presto Verifier can be used to test Presto against another database (such as MySQL),
or to test two Presto clusters against each other. We use it to continuously test trunk
against the previous release while developing Presto.

Running Verifier
----------------

Create a MySQL database with the following table and load it with the queries you would like to run:

.. code-block:: sql

    CREATE TABLE verifier_queries (
        id int(11) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
        suite varchar(256) NOT NULL,
        name varchar(256) DEFAULT NULL,
        control_catalog varchar(256) NOT NULL,
        control_schema varchar(256) NOT NULL,
        control_query text NOT NULL,
        test_catalog varchar(256) NOT NULL,
        test_schema varchar(256) NOT NULL,
        test_query text NOT NULL,
        control_username varchar(256) NOT NULL DEFAULT 'verifier-test',
        control_password varchar(256) DEFAULT NULL,
        test_username varchar(256) NOT NULL DEFAULT 'verifier-test',
        test_password varchar(256) DEFAULT NULL,
        session_properties_json varchar(2048) DEFAULT NULL)

Next, create a properties file to configure the verifier:

.. code-block:: none

    source-query.suite=my_suite
    source-query.database=jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password
    control.gateway=jdbc:presto://localhost:8080
    test.gateway=jdbc:presto://localhost:8081
    test-id=1

Lastly, download :maven_download:`verifier`, rename it to ``verifier``,
make it executable with ``chmod +x``, then run it:

.. code-block:: none

    ./verifier verify config.properties


Configuration Reference
-----------------------

================================= =======================================================================
Name                              Description
================================= =======================================================================
``control.timeout``               The maximum execution time of the control queries.
``test.timeout``                  The maximum execution time of the test queries.
``metadata.timeout``              The maximum execution time of the queries that are required for
                                  obtaining table metadata or rewriting queries.
``checksum.timeout``              The maximum execution time of the queries that computes checksum for
                                  the control and the test results.
``whitelist``                     A comma-separated list that specifies names of the queries within the
                                  suite to verify.
``blacklist``                     A comma-separated list that specifies names of the queries to be
                                  excluded from suite. ``blacklist`` is applied after ``whitelist``.
``source-query.table-name``       Specifies the MySQL table from which to read the source queries for
                                  verification.
``event-clients``                 A comma-separated list that specifies where the output events should be
                                  emitted. Valid individual values are ``json`` and ``human-readable``.
``json.log-file``                 Specifies the output files for JSON events. If ``json`` is specified in
                                  ``event-clients`` but this property is not set, JSON events are emitted
                                  to ``stdout``.
``human-readable.log-file``       Specifies the output files for human readable events. If
                                  ``human-readable`` is specified in ``event-clients`` but this property
                                  is not set, human readable events are emitted to ``stdout``.
``max-concurrency``               Specifies the maximum concurrent verification. Alternatively speaking,
                                  the maximum concurrent queries that will be submitted to control and
                                  test clusters combined.
``relative-error-margin``         Specified the maximum tolerable relative error between control and test
                                  queries for floating point columns.
================================= =======================================================================
