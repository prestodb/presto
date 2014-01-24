============
Release 0.57
============

Distinct Aggregations
---------------------

The ``DISTINCT`` argument qualifier for aggregation functions is now
fully supported. For example::

    SELECT country, count(DISTINCT city), count(DISTINCT age)
    FROM users
    GROUP BY country

.. note::

    :func:`approx_distinct` should be used in preference to this
    whenever an approximate answer is allowable as it is substantially
    faster and does not have any limits on the number of distinct items it
    can process. ``COUNT(DISTINCT ...)`` must transfer every item over the
    network and keep each distinct item in memory.

Hadoop 2.x
----------

Use the ``hive-hadoop2`` connector to read Hive data from Hadoop 2.x.
See :doc:`/installation/deployment` for details.

Amazon S3
---------

All Hive connectors support reading data from
`Amazon S3 <http://aws.amazon.com/s3/>`_.
This requires two additional catalog properties for the Hive connector
to specify your AWS Access Key ID and Secret Access Key:

.. code-block:: none

    hive.s3.aws-access-key=AKIAIOSFODNN7EXAMPLE
    hive.s3.aws-secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

Miscellaneous
-------------

* Allow specifying catalog and schema in the :doc:`/installation/jdbc` URL.

* Implement more functionality in the JDBC driver.

* Allow certain custom ``InputFormat``\s to work by propagating
  Hive serialization properties to the ``RecordReader``.

* Many execution engine performance improvements.

* Fix optimizer performance regression.

* Fix weird ``MethodHandle`` exception.
