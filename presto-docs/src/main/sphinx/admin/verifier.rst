===============
Presto Verifier
===============

Presto Verifier is a tool to run queries and verify correctness. It can be used to test whether a
new Presto version produces the correct query results, or to test if pairs of Presto queries have
the same semantics.

During each Presto release, Verifier is run to ensure that there is no correctness regression.

Using Verifier
--------------

In a MySQL database, create the following table and load it with the queries you would like to run:

.. code-block:: sql

    CREATE TABLE verifier_queries (
        id int(11) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
        suite varchar(256) NOT NULL,
        name varchar(256) DEFAULT NULL,
        control_catalog varchar(256) NOT NULL,
        control_schema varchar(256) NOT NULL,
        control_query text NOT NULL,
        control_username varchar(256) DEFAULT NULL,
        control_password varchar(256) DEFAULT NULL,
        control_session_properties text DEFAULT NULL,
        test_catalog varchar(256) NOT NULL,
        test_schema varchar(256) NOT NULL,
        test_query text NOT NULL,
        test_username varchar(256) DEFAULT NULL,
        test_password varchar(256) DEFAULT NULL,
        test_session_properties text DEFAULT NULL)

Next, create a ``config.properties`` file:

.. code-block:: none

    source-query.suites=suite
    source-query.database=jdbc:mysql://localhost:3306/mydb?user=my_username&password=my_password
    control.hosts=127.0.0.1
    control.http-port=8080
    control.jdbc-port=8080
    control.application-name=verifier-test
    test.hosts=127.0.0.1
    test.http-port=8081
    test.jdbc-port=8081
    test.application-name=verifier-test
    test-id=1

Download :maven_download:`verifier` and rename it to ``verifier.jar``. To run the Verifier:

.. code-block:: none

    java -Xmx1G -jar verifier.jar verify config.properties


Verifier Procedures
-------------------

The following steps summarize the workflow of Verifier.

* **Importing Source Queries**
   * Reads the list of source queries (query pairs with configuration) from the MySQL table.

* **Query Pre-processing and Filtering**
   * Applies overrides to the catalog, schema, username, and password of each query.
   * Filters queries according to whitelist and blacklist. Whitelist is applied before blacklist.
   * Filters out queries with invalid syntax.
   * Filters out queries not supported for validation. ``Select``, ``Insert``, and
     ``CreateTableAsSelect`` are supported.

* **Query rewriting**
    * Rewrites queries before execution to ensure that production data is not modified.
    * Rewrites ``Select`` queries to ``CreateTableAsSelect``
       * Column names are determined by running the ``Select`` query with ``LIMIT 0``.
       * Artificial names are used for unnamed columns.
    * Rewrites ``Insert`` and ``CreateTableAsSelect`` queries to have their table names replaced.
       * Constructs a setup query to create the table necessary for an ``Insert`` query.

* **Query Execution**
    * Conceptually, Verifier is configured with a control cluster and a test cluster. However, they
      may be pointed to the same Presto cluster for certain tests.
    * For each source query, executes the following queries in order.
        * Control setup queries
        * Control query
        * Test setup queries
        * Test query
        * Control and test teardown queries
    * Queries are subject to timeouts and retries.
        * Cluster connection failures and transient Presto failures are retried.
        * Query retries may conceal reliability issues, and therefore Verifier records all
          occurred Presto query failures, including the retries.
    * Certain query failures are automatically submitted for re-validation, such as partition
      dropped or table dropped during query.
    * See `Failure Resolution`_ for auto-resolving of query failures.

* **Results Comparison**
    * For ``Select``, ``Insert``, and ``CreateTableAsSelect`` queries, results are written into
      temporary tables.
    * Constructs and runs the checksum queries for both control and test.
    * Verifies table schema and row count are the same for the control and the test result table.
    * Verifies checksums are matching for each column. See `Column Checksums`_ for special handling
      of different column types.
    * See `Determinism`_ for handling of non-deterministic queries.

* **Emitting Results**
    * Verification results can be exported as ``JSON``, or human readable text.

Column Checksums
----------------
For each column in the control/test query, one or more columns are generated in the checksum
queries.

* **Floating Point Columns**
    * For ``DOUBLE`` and ``REAL`` columns, 4 columns are generated for verification:
       * Sum of the finite values of the column
       * ``NAN`` count of the column
       * Positive infinity count of the column
       * Negative infinity count of the column
    * Checks if ``NAN`` count, positive and negative infinity count matches.
    * Checks the nullity of control sum and test sum.
    * If either control mean or test mean very close 0, checks if both are close to 0.
    * Checks the relative error between control sum and test sum.
* **Array Columns**
    * 2 columns are generated for verification:
       * Sum of the cardinality
       * Array checksum
    * For an array column ``arr`` of type ``array(E)``:
       * If ``E`` is not orderable, array checksum is ``checksum(arr)``.
       * If ``E`` is orderable, array checksum ``coalesce(checksum(try(array_sort(arr))), checksum(arr))``.
* **Map Columns**
    * 4 columns are generated for verification:
       * Sum of the cardinality
       * Checksum of the map
       * Array checksum of the key set
       * Array checksum of the value set
* **Row Columns**
    * Checksums row fields recursively according to the type of the fields.
* For all other column types, generates a simple checksum using the :func:`checksum` function.

Determinism
-----------
A result mismatch, either a row count mismatch or a column mismatch, can be caused by
non-deterministic query features. To avoid false alerts, we perform determinism analysis
for the control query. If a query is found non-deterministic, we skip the verification as it
does not provide insights.

Determinism analysis follows the following steps. If a query is found non-deterministic at any
point, the analysis will conclude.

* Non-deterministic catalogs can be specified with ``determinism.non-deterministic-catalog``.
  If a query references any table from those catalogs, the query is considered non-deterministic.
* Runs the control query again and compares the results with the initial control query run.
* If a query has a ``LIMIT n`` clause but no ``ORDER BY`` clause at the top level:
   * Runs a query to count the number of rows produced by the control query without the ``LIMIT``
     clause.
   * If the resulting row count is greater than ``n``, treats the control query as
     non-deterministic.

Failure Resolution
------------------
The differences in configuration, including cluster size, can cause a query to succeed on the
control cluster but fail on the test cluster. A checksum query can also fail, which may be due to
limitation of Presto or Presto Verifier. Thus, we allow Verifier to automatically resolve certain
query failures.

* ``EXCEEDED_GLOBAL_MEMORY_LIMIT``: Resolves if the control query uses more memory than the test
  query.
* ``EXCEEDED_TIME_LIMIT``: Resolves unconditionally.
* ``TOO_MANY_HIVE_PARTITIONS``: Resolves if the test cluster does not have enough workers to make
  sure the number of partitions assigned to each worker stays within the limit.
* ``COMPILER_ERROR``, ``GENERATED_BYTECODE_TOO_LARGE``: Resolves if the control checksum query
  fails with this error. If the control query has too many columns, generated checksum queries
  might be too large in certain cases.

In cases of result mismatches, Verifier may be giving noisy signals, and we allow Verifier to
automatically resolve certain mismatches.

* **Structured-typed Columns**: If array element or map key/value contains floating point types, column checksum is unlikely to match.
    * For an array column, resolve if the element type contains floating point types and the
      cardinality checksum matches.
    * For a map column, resolve the mismatch when both of the following conditions are true:
       * The cardinality checksum matches.
       * The checksum of the key or value that does not contains floating point types matches.
    * Resolve a test case only when all columns are resolved.
* **Resolved Functions**: In the case of a results mismatch, if the query uses a function in a
    specified list, the test case is marked as resolved.

Explain Mode
------------
In explain mode, Verifier checks whether source queries can be explained instead of whether
they produces the same results. Verification is marked as succeeded when both control query
and test query can be explained.

The field ``matchType`` in the output event can be used as an indicator whether there are
plan differences between the control run and the test run.

For non-DML queries, the control query and the plan comparison are skipped.

Extending Verifier
------------------

Verifier can be extended for further behavioral changes in addition to configuration properties.

`AbstractVerifyCommand <https://github.com/prestodb/presto/blob/master/presto-verifier/src/main/java/com/facebook/presto/verifier/framework/AbstractVerifyCommand.java>`_
shows the components that be extended. Implement the abstract class and create a command line wrapper similar to
`PrestoVerifier <https://github.com/prestodb/presto/blob/master/presto-verifier/src/main/java/com/facebook/presto/verifier/PrestoVerifier.java>`_.


Configuration Reference
-----------------------

General Configuration
~~~~~~~~~~~~~~~~~~~~~

=========================================== ===============================================================================
Name                                        Description
=========================================== ===============================================================================
``whitelist``                               A comma-separated list specifying the names of the queries within the suite
                                            to verify.
``blacklist``                               A comma-separated list specifying the names of the queries to be excluded
                                            from the suite. ``blacklist`` is applied after ``whitelist``.
``source-query-supplier``                   The name of the source query supplier. Supports ``mysql``.
``source-query.table-name``                 The name of the table that holds verifier queries. Available only when
                                            ``source-query-supplier`` is ``mysql``.
``event-clients``                           A comma-separated list specifying where the output events should be emitted.
                                            Supports ``json`` and ``human-readable``.
``json.log-file``                           The output files of ``JSON`` events. If not set, ``JSON`` events are emitted to
                                            ``stdout``.
``human-readable.log-file``                 The output files for human-readable events. If not set, human-readable events
                                            are emitted to ``stdout``.
``control.table-prefix``                    The table prefix to be appended to the control target table.
``test.table-prefix``                       The table prefix to be appended to the test target table.
``test-id``                                 A string to be attached to output events.
``max-concurrency``                         Maximum number of concurrent verifications.
``suite-repetition``                        How many times a suite is verified.
``query-repetition``                        How many times a source query is verified.
``relative-error-margin``                   Maximum tolerable relative error between control sum and test sum of a
                                            floating point column.
``absolute-error-margin``                   Floating point averages that are below this threshold are treated as ``0``.
``run-teardown-on-result-mismatch``         Whether to run teardown query in case of result mismatch.
``verification-resubmission.limit``         A limit on how many times a source query can be re-submitted for verification.
=========================================== ===============================================================================


Query Override Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The following configurations control the behavior of query metadata modification before verification starts.
Counterparts are also available for test queries with prefix ``control`` being replaced with ``test``.

================================================ ===============================================================================
Name                                             Description
================================================ ===============================================================================
``control.catalog-override``                     The catalog to be applied to all queries if specified.
``control.schema-override``                      The schema to be applied to all queries if specified.
``control.username-override``                    The username to be applied to all queries if specified.
``control.password-override``                    The password to be applied to all queries if specified.
``control.session-properties-override-strategy`` Supports 3 values. ``NO_ACTION``: Use the session properties as specified for
                                                 each query. ``OVERRIDE``: Merge the session properties of each query with the
                                                 override, with override being the dominant. ``SUBSTITUTE``, The session
                                                 properties of each query is replaced with the override.
``control.session-properties-override``          The session property to be applied to all queries.
================================================ ===============================================================================

Query Execution Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following configurations control the behavior of query execution on the control cluster.
Counterparts are also available for test clusters with prefix ``control`` being replaced with ``test``.

=========================================== ===============================================================================
Name                                        Description
=========================================== ===============================================================================
``control.hosts``                           Comma-separated list of the control cluster hostnames or IP addresses.
``control.jdbc-port``                       JDBC port of the control cluster.
``control.http-host``                       HTTP port of the control cluster.
``control.jdbc-url-parameters``             A ``JSON`` map representing the additional URL parameters for control JDBC.
``control.query-timeout``                   The execution time limit of the control and the test queries.
``control.metadata-timeout``                The execution time limit of ``DESC`` queries and ``LIMIT 0`` queries.
``control.checksum-timeout``                The execution time limit of checksum queries.
``control.application-name``                ApplicationName to be passed in ClientInfo. Can be used to set source.
=========================================== ===============================================================================

Determinism Analyzer Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=========================================== ===============================================================================
Name                                        Description
=========================================== ===============================================================================
``determinism.run-teardown``                Whether to run teardown queries for tables produced in determinism analysis.
``determinism.max-analysis-runs``           Maximum number of additional control runs to check for the determinism of the
                                            control query.
``determinism.handle-limit-query``          Whether to enable the special handling for queries with a top level ``LIMIT``
                                            clause.
``determinism.non-deterministic-catalogs``  A comma-separated list of non-deterministic catalogs. Queries referencing table
                                            from those catalogs are treated as non-deterministic.
=========================================== ===============================================================================

Failure Resolution Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

========================================================= ======================================================================
Name                                                      Description
========================================================= ======================================================================
``exceeded-global-memory-limit.failure-resolver.enabled`` Whether to enable the failure resolver for test query failures with
                                                          ``EXCEEDED_GLOBAL_MEMORY_LIMIT``.
``exceeded-time-limit.failure-resolver.enabled``          Whether to enable the failure resolver for test query failures with
                                                          ``EXCEEDED_TIME_LIMIT``.
``verifier-limitation.failure-resolver.enabled``          Whether to enable the failure resolver for failures due to Verifier
                                                          limitations.
``too-many-open-partitions.failure-resolver.enabled``     Whether to enable the failure resolver for test query failures with
                                                          ``HIVE_TOO_MANY_OPEN_PARTITIONS``.
``too-many-open-partitions.max-buckets-per-writer``       The maximum buckets count per writer configured on the control and the
                                                          test cluster.
``too-many-open-partitions.cluster-size-expiration``      The time limit of the test cluster size being cached.
``structured-column.failure-resolver.enabled``            Whether to enable the failure resolver for column mismatches of
                                                          structured-type columns.
``ignored-functions.failure-resolver.enabled``            Whether to enable the ``IgnoredFunctions`` result mismatch failure
                                                          resolver.
``ignored-functions.functions``                           A comma-separated list of functions. Resolves mismatches if a query
                                                          uses any functions in the list.
========================================================= ======================================================================
