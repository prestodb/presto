============
SPI Overview
============

When you implement a new Presto plugin, you implement interfaces and
override methods defined by the SPI.

Plugins can provide connectors, which are the source of all data
for queries in Presto. Even if your data source doesn't have underlying
tables backing it, as long as you adapt your data source to the API
expected by Presto, you can write queries against this data.

Code
----

The SPI source can be found in the ``presto-spi`` directory in the
root of the Presto source tree.

Plugin Metadata
---------------

Each plugin identifies an entry point: an implementation of the
``Plugin`` interface. This class name is provided to Presto via
the standard Java ``ServiceLoader`` interface: the classpath contains
a resource file named ``com.facebook.presto.spi.Plugin`` in the
``META-INF/services`` directory. The content of this file is a
single line listing the name of the plugin class:

.. code-block:: none

    com.facebook.presto.example.ExamplePlugin

Plugin
------

The ``Plugin`` interface is a good starting place for developers looking
to understand the Presto SPI. The ``getConnectorFactories()`` method is a top-level
function that Presto calls to retrieve a ``ConnectorFactory`` when Presto
is ready to create an instance of a connector to back a catalog.

ConnectorFactory
----------------

Instances of your connector are created by a ``ConnectorFactory``
instance which is created when Presto calls ``getConnectorFactories()`` on the
plugin to request a service of type ``ConnectorFactory``.
The connector factory is a simple interface responsible for creating an
instance of a ``Connector`` object that returns instances of the
following services:

* ``ConnectorMetadata``
* ``ConnectorSplitManager``
* ``ConnectorHandleResolver``
* ``ConnectorRecordSetProvider``

ConnectorMetadata
^^^^^^^^^^^^^^^^^

The connector metadata interface has a large number of important
methods that are responsible for allowing Presto to look at lists of
schemas, lists of tables, lists of columns, and other metadata about a
particular data source.

This interface is too big to list in this documentation, but if you
are interested in seeing strategies for implementing these methods,
look at the :doc:`example-http` and the Cassandra connector. If
your underlying data source supports schemas, tables and columns, this
interface should be straightforward to implement. If you are attempting
to adapt something that is not a relational database (as the Example HTTP
connector does), you may need to get creative about how you map your
data source to Presto's schema, table, and column concepts.

ConnectorSplitManger
^^^^^^^^^^^^^^^^^^^^

The split manager partitions the data for a table into the individual
chunks that Presto will distribute to workers for processing.
For example, the Hive connector lists the files for each Hive
partition and creates one or more split per file.
For data sources that don't have partitioned data, a good strategy
here is to simply return a single split for the entire table. This
is the strategy employed by the Example HTTP connector.

ConnectorRecordSetProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^

Given a split and a list of columns, the record set provider is
responsible for delivering data to the Presto execution engine.
It creates a ``RecordSet``, which in turn creates a ``RecordCursor``
that is used by Presto to read the column values for each row.
