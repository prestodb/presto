=============
JMX Connector
=============

The JMX connector provides the ability to query JMX information from all
nodes in a Presto cluster. This is very useful for monitoring or debugging.
Java Management Extensions (JMX) provides information about the Java
Virtual Machine and all of the software running inside it. Presto itself
is heavily instrumented via JMX.

This connector can also be configured so that chosen JMX information will
be periodically dumped and stored in memory for later access.

Configuration
-------------

To configure the JMX connector, create a catalog properties file
``etc/catalog/jmx.properties`` with the following contents:

.. code-block:: none

    connector.name=jmx

To enable periodical dumps, define the following properties:

.. code-block:: none

    connector.name=jmx
    jmx.dump-tables=java.lang:type=Runtime,com.facebook.presto.execution.scheduler:name=NodeScheduler
    jmx.dump-period=10s
    jmx.max-entries=86400

``dump-tables`` is a comma separated list of Managed Beans (MBean). It specifies
which MBeans will be sampled and stored in memory every ``dump-period``.
History will have limited size of ``max-entries`` of entries. Both ``dump-period``
and ``max-entries`` have default values of ``10s`` and ``86400`` accordingly.

Commas in MBean names should be escaped in the following manner:

.. code-block:: none

    connector.name=jmx
    jmx.dump-tables=com.facebook.presto.memory:type=memorypool\\,name=general,\
       com.facebook.presto.memory:type=memorypool\\,name=system,\
       com.facebook.presto.memory:type=memorypool\\,name=reserved

Querying JMX
------------

The JMX connector provides two schemas.

The first one is ``current`` that contains every MBean from every node in the Presto
cluster. You can see all of the available MBeans by running ``SHOW TABLES``::

    SHOW TABLES FROM jmx.current;

MBean names map to non-standard table names and must be quoted with
double quotes when referencing them in a query. For example, the
following query shows the JVM version of every node::

    SELECT node, vmname, vmversion
    FROM jmx.current."java.lang:type=runtime";

.. code-block:: none

                     node                 |              vmname               | vmversion
    --------------------------------------+-----------------------------------+-----------
     ddc4df17-0b8e-4843-bb14-1b8af1a7451a | Java HotSpot(TM) 64-Bit Server VM | 24.60-b09
    (1 row)

The following query shows the open and maximum file descriptor counts
for each node::

    SELECT openfiledescriptorcount, maxfiledescriptorcount
    FROM jmx.current."java.lang:type=operatingsystem";

.. code-block:: none

     openfiledescriptorcount | maxfiledescriptorcount
    -------------------------+------------------------
                         329 |                  10240
    (1 row)

The ``history`` schema contains the list of tables configured in the connector properties file.
The tables have the same columns as those in the current schema, but with an additional
timestamp column that stores the time at which the snapshot was taken::

    SELECT "timestamp", "uptime" FROM jmx.history."java.lang:type=runtime";

.. code-block:: none

            timestamp        | uptime
    -------------------------+--------
     2016-01-28 10:18:50.000 |  11420
     2016-01-28 10:19:00.000 |  21422
     2016-01-28 10:19:10.000 |  31412
    (3 rows)
