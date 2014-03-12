============
Release 0.61
============

Add support for Table Value Constructors
----------------------------------------

Presto now supports the SQL table value constructor syntax to create inline tables.
The ``VALUES`` clause can be used anywhere a ``SELECT`` statement is allowed.
For example, as a top-level query::

   VALUES ('a', 1), ('b', 2);

.. code-block:: none

    _col0 | _col1
   -------+-------
    a     |     1
    b     |     2
   (2 rows)

Alternatively, in the ``FROM`` clause::

    SELECT *
    FROM (
      VALUES
        ('a', 'ape'),
        ('b', 'bear')
    ) AS animal (letter, animal)
    JOIN (
      VALUES
        ('a', 'apple'),
        ('b', 'banana')
    ) AS fruit (letter, fruit)
    USING (letter);

.. code-block:: none

    letter | animal | letter |  fruit
   --------+--------+--------+---------
    a      | ape    | a      | apple
    b      | bear   | b      | banana
   (2 rows)


Cassandra
---------

* Add support for upper-case schema, table, and columns names.

* Add support for ``DECIMAL`` type.

Amazon S3 support
-----------------

* Completely rewritten Hadoop FileSystem implementation for S3 using the Amazon AWS SDK,
  with major performance and reliability improvements.

* Add support for writing data to S3.

Approximate Aggregation Queries
-------------------------------

We have added experimental support for aggregate queries that return
approximate results with error bounds. This feature is designed to be
used with sampled tables generated using the ``TABLESAMPLE POISSONIZED RESCALED``.
For example, the following query will create a 1% sample::

    CREATE TABLE lineitems_sample AS
    SELECT *
    FROM tpch.sf10.lineitems TABLESAMPLE POISSONIZED (1) RESCALED

Then, to run an approximate query::

    SELECT COUNT(*)
    FROM lineitems_sample
    APPROXIMATE AT 95.0 CONFIDENCE


.. code-block:: none

               _col0
    ----------------------------
     5.991790345E7 +/- 14835.75
    (1 row)


To enable this feature you must add ``analyzer.experimental-syntax-enabled=true`` to your config.

.. note::

    The syntax and functionality for approximate queries is experimental and will likely
    change in future versions.


Miscellaneous
-------------

* General improvements to the JDBC driver, specifically with respect to metadata handling.

* Fix division by zero errors in variance aggregation functions (``VARIANCE``, ``STDDEV``, etc.).

* Fix a bug when using ``DISTINCT`` aggregations in the ``HAVING`` clause.

* Fix an out of memory issue when writing large tables.

* Fix a bug when using ``ORDER BY rand()`` in a ``JOIN`` query.

* Fix handling of timestamps in maps and lists in Hive connector.

* Add instrumentation for Hive metastore and HDFS API calls to track failures and latency. These metrics are exposed via JMX.
