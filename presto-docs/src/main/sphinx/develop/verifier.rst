==================
Verifier Mechanics
==================

The Presto Verifier can be used to compare two versions of Presto. It validates whether the outputs of Presto queries are the same across different version.
Additionally, one can perform other kinds of analysis, such as performance analysis based on the execution information of the queries that were run.

Procedures and Functionalities
------------------------------

The following steps summarize the working of Verifier.

Importing Source Queries
~~~~~~~~~~~~~~~~~~~~~~~~

* Source queries can be read through MySQL, provided that there is an xdb table with with the minimum required columns defined in :ref:`running-verifier`.
* User-specified source query supplier one can also be plugged in.

Query Pre-processing and Filtering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Verifier applies catalog/schema/username/password overrides if they are specified.
* Queries within a suite can be selected and/or excluded by specifying the ``whitelist`` and/or the ``blacklist`` configuration properties.
* Verifier filters queries with invalid syntax.
* User-specified custom filters can also be plugged in.

Query rewriting
~~~~~~~~~~~~~~~
* Each query is rewritten before execution to ensure that verification does not interfere with production data.
    * Select queries are rewritten as CreateTableAsSelect.
    * Insert and CreateTableAsSelect queries have their target table shadowed with prefixes and/or different catalog/schema.

Query Execution
~~~~~~~~~~~~~~~
* For each source query, the following queries are executed in the mentioned order:
    * Control setup queries
    * Control query
    * Test setup queries
    * Test query
    * Control and Test teardown
* Control and test clusters need to be Presto clusters and are specified using the JDBC connection URL.
* Query timeouts can be configured.
* Connection failures and transient failures are retried. Retry can be configured. The list of retryable transient failures can be overridden by a customized plugin.
* To avoid query retry hiding reliability issues, we keep track of and emit all occurred failures regardless of whether it succeeds after retries.

Results Comparison
~~~~~~~~~~~~~~~~~~
* For all 3 query types (``Select``, ``Insert``, and ``CreateTableAsSelect``), results produced by queries are written into temporary tables,
  and thus we run checksum queries against those tables.
* We verify the schema of the tables, then the row count and the column checksums.
* For a floating point columns ``f``, we have special handling to accommodate discrepancy incurred by the difference in the order of arithmetic operation execution. We generate 4 checksum columns:
    * ``sum(f) FILTER (WHERE is_finite(f)) f_sum``
    * ``count(f) FILTER (WHERE is_nan(f)) f_nan_count``
    * ``count(f) FILTER (WHERE f = infinity()) f_positive_infinity_count``
    * ``count(f) FILTER (WHERE f = -infinity()) f_negative_infinity_count``

    ``NaN`` and ``infinity`` counts need to match. For sum, the following needs to hold true for a match:
    ``abs(controlSum - testSum) / min((abs(controlSum) + abs(testSum)) / 2 < relativeErrorMargin``
* For an array column ``a``, we treat them as order-insensitively by comparing ``array_sort(checksum(a))``
* For any other column ``c``, we compare ``checksum(c)``

Non-Determinism
~~~~~~~~~~~~~~~
* In case of result mismatch due to row count mismatch or column mismatch. We rerun the control queries 2 more times.
  Along with the initial control query results, we determine whether the control query is deterministic.
* The 2nd additional run will only be started if the first two runs match.

Automatic Failure Resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* Because there are potential configuration differences between the control and the test clusters, certain queries failures are automatically resolved:
    * Global memory exceeded is automatically resolved if the control query uses more memory than the test query does.
    * Time limit exceeded is automatically resolved.
* Custom failure resolver can be plugged in.

Emitting Results
~~~~~~~~~~~~~~~~
* Verification results can be exported as JSON, or human readable text.
* Custom EventClient can be plugged in to perform additional result exports.

Extending Verifier
------------------

The Verifier can be configured through various configuration, and when needed, it can also be easily extended for further behavioral change.
To implement a extension, implement an `AbstractVerifyCommand <https://github.com/prestodb/presto/blob/master/presto-verifier/src/main/java/com/facebook/presto/verifier/framework/AbstractVerifyCommand.java>`_,
similar to `PrestoVerifyCommand <https://github.com/prestodb/presto/blob/master/presto-verifier/src/main/java/com/facebook/presto/verifier/PrestoVerifyCommand.java>`_,
, and then create a command line wrapper similar to `PrestoVerifier <https://github.com/prestodb/presto/blob/master/presto-verifier/src/main/java/com/facebook/presto/verifier/PrestoVerifier.java>`_.
