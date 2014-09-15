============
Release 0.76
============

Cassandra Changes
-----------------

The :doc:`/connector/cassandra` configuration properties
``cassandra.client.read-timeout`` and ``cassandra.client.connect-timeout``
are now specified using a duration rather than milliseconds (this makes
them consistent with all other such properties in Presto). If you were
previously specifying a value such as ``25``, change it to ``25ms``.
