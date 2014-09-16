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

Index Join Changes
------------------

* Index joins can now be executed in distributed mode by enabling the
  ``distributed-index-joins-enabled`` config option.

* Each index join now executes in a separate memory allocation space than the
  standard task allocation. This memory size defaults to 64MB and can be tuned
  with the ``task.max-index-memory`` config option.

* Index join executions will now flush out pre-existing indexed data from memory
  to make space for newly fetched index results (if needed). Index fetch batch
  sizes will also be dynamically adjusted to fit the results in memory.

Query Throttling
----------------

Presto now bounds the maxmimum number of concurrently executed queries. This
value is tunable with the ``query.max-concurrent-queries`` option
(default: 1000). Additional queries will be queued up for execution up to the
limit set by ``query.max-queued-queries`` (default: 5000).

General Changes
---------------

* Fixed a bug whereby the coordinator could lose track of a scheduled worker task.
