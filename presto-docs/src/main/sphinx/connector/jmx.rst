=============
JMX Connector
=============

The JMX connector provides the ability to query JMX information from all
nodes in a Presto cluster. This is very useful for monitoring or debugging.
Java Management Extensions (JMX) provides information about the Java
Virtual Machine and all of the software running inside it. Presto itself
is heavily instrumented via JMX.

Configuration
-------------

To configure the JMX connector, create a catalog properties file
``etc/catalog/jmx.properties`` with the following contents:

.. code-block:: none

    connector.name=jmx

Querying JMX
------------

The JMX connector provides a single schema ``jmx`` that contains
every Managed Bean (MBean) from every node in the Presto cluster.
You can see all of the available MBeans by running ``SHOW TABLES``::

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
