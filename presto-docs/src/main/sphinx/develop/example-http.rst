======================
Example HTTP Connector
======================

The Example HTTP connector has a simple goal: it reads comma-separated
data over HTTP. For example, if you have a large amount of data in a
CSV format, you can point the example HTTP connector at this data and
write a SQL query to process it.

Code
----

The Example HTTP connector can be found in the ``presto-example-http``
directory in the root of the Presto source tree.

Plugin Implementation
---------------------

The plugin implementation in the Example HTTP connector looks very
similar to other plugin implementations.  Most of the implementation is
devoted to handling optional configuration and the only function of
interest is the following:

.. code-block:: java

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ExampleConnectorFactory());
    }

Note that the ``ImmutableList`` class is a utility class from Guava.

As with all connectors, this plugin overrides the ``getConnectorFactories()`` method
and returns an ``ExampleConnectorFactory``.

ConnectorFactory Implementation
-------------------------------

In Presto, the primary object that handles the connection between
Presto and a particular type of data source is the ``Connector`` object,
which are created using ``ConnectorFactory``.

This implementation is available in the class ``ExampleConnectorFactory``.
The first thing the connector factory implementation does is specify the
name of this connector. This is the same string used to reference this
connector in Presto configuration.

.. code-block:: java

    @Override
    public String getName()
    {
        return "example-http";
    }

The real work in a connector factory happens in the ``create()``
method. In the ``ExampleConnectorFactory`` class, the ``create()`` method
configures the connector and then asks Guice to create the object.
This is the meat of the ``create()`` method without parameter validation
and exception handling:

.. code-block:: java

    // A plugin is not required to use Guice; it is just very convenient
    Bootstrap app = new Bootstrap(
            new JsonModule(),
            new ExampleModule(connectorId));

    Injector injector = app
            .strictConfig()
            .doNotInitializeLogging()
            .setRequiredConfigurationProperties(requiredConfig)
            .initialize();

    return injector.getInstance(ExampleConnector.class);

Connector: ExampleConnector
^^^^^^^^^^^^^^^^^^^^^^^^^^^

This class allows Presto to obtain references to the various services
provided by the connector.

Metadata: ExampleMetadata
^^^^^^^^^^^^^^^^^^^^^^^^^

This class is responsible for reporting table names, table metadata,
column names, column metadata and other information about the schemas
that are provided by this connector. ``ConnectorMetadata`` is also called
by Presto to ensure that a particular connector can understand and
handle a given table name.

The ``ExampleMetadata`` implementation delegates many of these calls to
``ExampleClient``, a class that implements much of the core functionality
of the connector.

Split Manager: ExampleSplitManager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The split manager partitions the data for a table into the individual
chunks that Presto will distribute to workers for processing.
In the case of the Example HTTP connector, each table contains one or
more URIs pointing at the actual data. One split is created per URI.

Record Set Provider: ExampleRecordSetProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The record set provider creates a record set which in turn creates a
record cursor that returns the actual data to Presto.
``ExampleRecordCursor`` reads data from a URI via HTTP. Each line
corresponds to a single row. Lines are split on comma into individual
field values which are then returned to Presto.
