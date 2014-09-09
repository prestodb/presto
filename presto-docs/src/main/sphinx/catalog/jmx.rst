===========
JMX Catalog
===========

The JMX catalog provides the ability to query JMX information from all
nodes in a Presto cluster.

Configuration
-------------

To configure the JMX connector create a properties file named
``jmx.properties`` in the ``etc/catalog`` directory with the following
content:

.. code-block:: none
    
    connector.name=jmx

Note that the JMX connector ships with Presto. No plugins or
connectors need to be installed to activate the JMX connector.

Using the JMX Schema
--------------------

While JMX is configured as a catalog there is really just one schema
named "jmx" within this catalog. To use the jmx schema, run:

.. code-block:: none

    use schema jmx

Running ``show tables`` will show you every JMX MBean available across
JVMs in a Presto cluster.

