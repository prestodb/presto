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

.. _Apache Kafka: http://kafka.apache.org/

Cassandra Changes
-----------------

The :doc:`/connector/cassandra` configuration properties
``cassandra.client.read-timeout`` and ``cassandra.client.connect-timeout``
are now specified using a duration rather than milliseconds (this makes
them consistent with all other such properties in Presto). If you were
previously specifying a value such as ``25``, change it to ``25ms``.

General Changes
---------------

* Fix hang in verifier, if an exception occurred
* Fix :func:`chr` function
* Fix JDBC client hanging the JVM on shutdown
