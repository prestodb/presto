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

To enable periodical dumps, define following properties:

.. code-block:: none

    jmx.dump-tables=java.lang:type=Runtime,com.facebook.presto.execution.scheduler:name=NodeScheduler
    jmx.dump-period=10s
    jmx.eviction-limit=86400

``dump-tables`` is a comma separated list of Managed Beans (MBean) that will
be dumped every ``dump-period`` milliseconds. Dump will be with limited size
of ``eviction-limit`` number of entries per each MBean. Both ``dump-period``
and ``eviction-limit`` have default values of ``10s`` and ``86400``
accordingly.

Querying JMX
------------

The JMX connector provides two schemas.

First one is ``jmx`` that contains every MBean from every node in the Presto
cluster. You can see all of the available MBeans by running ``SHOW TABLES``::

    SHOW TABLES FROM jmx.jmx;

MBean names map to non-standard table names and must be quoted with
double quotes when referencing them in a query. For example, the
following query shows the JVM version of every node::

    SELECT node, vmname, vmversion
    FROM jmx.jmx."java.lang:type=runtime";

.. code-block:: none

                     node                 |              vmname               | vmversion
    --------------------------------------+-----------------------------------+-----------
     ddc4df17-0b8e-4843-bb14-1b8af1a7451a | Java HotSpot(TM) 64-Bit Server VM | 24.60-b09
    (1 row)

The following query shows the open and maximum file descriptor counts
for each node::

    SELECT openfiledescriptorcount, maxfiledescriptorcount
    FROM jmx.jmx."java.lang:type=operatingsystem";

.. code-block:: none

     openfiledescriptorcount | maxfiledescriptorcount
    -------------------------+------------------------
                         329 |                  10240
    (1 row)

Second schema is called ``history`` and contains dumped tables as configured
in ``jmx.properties``. It contains ``dump-tables`` with the exactly same
columns as original tables in ``jmx`` schema, but with added ``timestamp``
column, which holds value of timestamp when the given row was dumped::

    SELECT "timestamp", "uptime" FROM jmx.history."java.lang:type=runtime";

.. code-block:: none

            timestamp        | uptime
    -------------------------+--------
     2016-01-28 10:18:50.000 |  11420
     2016-01-28 10:19:00.000 |  21422
     2016-01-28 10:19:10.000 |  31412
    (3 rows)
