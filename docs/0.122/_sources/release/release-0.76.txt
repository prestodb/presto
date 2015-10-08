============
Release 0.76
============

Kafka Connector
---------------

This release adds a connector that allows querying of `Apache Kafka`_ topic data
from Presto. Topics can be live and repeated queries will pick up new data.

Apache Kafka 0.8+ is supported although Apache Kafka 0.8.1+ is recommended.
There is extensive :doc:`documentation </connector/kafka>` about configuring
the connector and a :doc:`tutorial </connector/kafka-tutorial>` to get started.

.. _Apache Kafka: https://kafka.apache.org/

MySQL and PostgreSQL Connectors
-------------------------------

This release adds the :doc:`/connector/mysql` and :doc:`/connector/postgresql`
for querying and creating tables in external relational databases. These can
be used to join or copy data between different systems like MySQL and Hive,
or between two different MySQL or PostgreSQL instances, or any combination.

Cassandra Changes
-----------------

The :doc:`/connector/cassandra` configuration properties
``cassandra.client.read-timeout`` and ``cassandra.client.connect-timeout``
are now specified using a duration rather than milliseconds (this makes
them consistent with all other such properties in Presto). If you were
previously specifying a value such as ``25``, change it to ``25ms``.

The retry policy for the Cassandra client is now configurable via the
``cassandra.retry-policy`` property. In particular, the custom ``BACKOFF``
retry policy may be useful.

Hive Changes
------------

The new :doc:`/connector/hive` configuration property ``hive.s3.socket-timeout``
allows changing the socket timeout for queries that read or write to Amazon S3.
Additionally, the previously added ``hive.s3.max-connections`` property
was not respected and always used the default of ``500``.

Hive allows the partitions in a table to have a different schema than the
table. In particular, it allows changing the type of a column without
changing the column type of existing partitions. The Hive connector does
not support this and could previously return garbage data for partitions
stored using the RCFile Text format if the column type was converted from
a non-numeric type such as ``STRING`` to a numeric type such as ``BIGINT``
and the actual data in existing partitions was not numeric. The Hive
connector now detects this scenario and fails the query after the
partition metadata has been read.

The property ``hive.storage-format`` is broken and has been disabled. It
sets the storage format on the metadata but always writes the table using
``RCBINARY``. This will be implemented in a future release.

General Changes
---------------

* Fix hang in verifier when an exception occurs.
* Fix :func:`chr` function to work with Unicode code points instead of ASCII code points.
* The JDBC driver no longer hangs the JVM on shutdown (all threads are daemon threads).
* Fix incorrect parsing of function arguments.
* The bytecode compiler now caches generated code for join and group byqueries,
  which should improve performance and CPU efficiency for these types of queries.
* Improve planning performance for certain trivial queries over tables with lots of partitions.
* Avoid creating large output pages. This should mitigate some cases of
  *"Remote page is too large"* errors.
* The coordinator/worker communication layer is now fully asynchronous.
  Specifically, long-poll requests no longer tie up a thread on the worker.
  This makes heavily loaded clusters more efficient.
