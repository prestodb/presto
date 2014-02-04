================
Example HTTP SPI
================

The example HTTP connector has a simple goal, it is a connector that is
configured to reference data over HTTP in a comma-separated format.
For example, if you have a large amount of data in a CSV format, you
can point the example HTTP connector at this data and write a query
against this data as if it were a table in a data source.

GitHub Code
-----------

The example HTTP SPI can be found here: 

    https://github.com/facebook/presto/tree/master/presto-example-http

Maven Project
-------------

The Example HTTP connector maintains a simple Maven Project Object
Model (POM) which is stored in the pom.xml file in this project.

Project Dependencies
^^^^^^^^^^^^^^^^^^^^

Using the example from GitHub, you can see that an SPI implementation
references a collection of dependencies using the provided
scope. These are all libraries which are supplied by Presto at runtime
but which are necessary for the compilation of a plugin implementing
the Presto SPI.

The following dependencies are included in the Example HTTP connector:

* com.presto.facebook:presto-spi
* io.airlift:bootstrap
* io.airlift:json
* io.airlift:log
* io.airlift:units
* io.airlift:configuration
* com.fasterxml.jackson.core:jackson-annotations
* com.google.guava:guava
* javax.inject:javax.inject
* com.google.inject:guice
* javax.validation:validation-api

Plugin Assembly Descriptor
^^^^^^^^^^^^^^^^^^^^^^^^^^

An SPI implementation is packaged into a plugin that is then deployed
to a Presto server. This plugin is packaged using a Maven Assembly
descriptor. This descriptor is stored in the
src/main/assembly/plugin.xml file and it dictates the structure of the
SPI archive.

This plugin descriptor sets the format of the SPI archive as a ZIP
file and it includes all runtime dependencies alongside the JAR file
which implements the SPI. In the case of the Example HTTP connector,
this will include two trivial logging libraries and the classes that
comprise the plugin.

.. code-block:: bash

    $ unzip presto-example-http-0.60-SNAPSHOT.zip 
    Archive:  presto-example-http-0.60-SNAPSHOT.zip
     extracting: slf4j-api-1.7.5.jar     
     extracting: slf4j-jdk14-1.7.5.jar   
     extracting: presto-example-http-0.60-SNAPSHOT.jar 

Plugin Implementation
---------------------

The plugin implementation in the example HTTP connector looks very
similar to other Plugin implementations.  Most of this implementation is
devoted to handling optional configuration, and the only function of
interest is the following:

.. code-block:: java

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(
	       type.cast(
	          new ExampleConnectorFactory(getOptionalConfig())));
        }
        return ImmutableList.of();
    }

Note that the ImmutableList class is a utility class from Google Guava
which was included as a provided dependency.

As with all connectors, this SPI overrides the getServices() method
and returns an ExampleConnectorFactory in response to a request for a
service of type ConnectorFactory.

ConnectorFactory Implementation
-------------------------------

In Presto, the primary object that handles the connection between
Presto and a particular type of data source is the Connector object.
The ConnectorFactory (predictably) deals with creating instances of
the Connector object.

This implementation is available in the class
ExampleConnectorFactory. The first thing the ConnectorFactory
implementation does is set the name of this connector. This is the
same string used to reference this connector in Presto coniguration.

.. code-block:: java

    @Override
    public String getName()
    {
        return "example-http";
    }

The real work in a ConnectorFactory object happens in the create()
method.  In the ExampleConnectorFactory class, the create method
configures the connector and then injects resources into the object.
Here's the meat of the create() method without parameter checking and
exception handling:

.. code-block:: java

    // A plugin is not required to use Guice; it is just 
    // very convenient
    Bootstrap app = new Bootstrap(
      new JsonModule(),
      new ExampleModule(connectorId));

    Injector injector = app
      .strictConfig()
      .doNotInitializeLogging()
      .setRequiredConfigurationProperties(requiredConfig)
      .setOptionalConfigurationProperties(optionalConfig)
      .initialize();

    ClassToInstanceMap<Object> services = ...

The omitted portion of this code simply injects instances of:

* ConnectorMetadata
* ConnectorSplitManager
* ConnectorRecordSetProvider
* ConnectorHandleResolver

The following sections explain the function and give a brief overview
of the example HTTP connector's implementation of each class.

Connector: ExampleConnector
^^^^^^^^^^^^^^^^^^^^^^^^^^^

This ExampleConnector class is a class that allows other services
and managers to get references to the various services provided by the
connector.

Metadata: ExampleMetadata
^^^^^^^^^^^^^^^^^^^^^^^^^

This class is responsible for reporting table names, table metadata,
column names, column metadata, and other information about the schemas
that are visible to this connector. ConnectorMetadata is also called
by Presto to ensure that a particular connector can understand and
handle a given table name.

The ExampleMetadata implementation delegates many of these calls to
the ExampleClient, an object to be explored in subsequent sections.

Split Manager: ExampleSplitManager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The split manager's job is to ask the underlying data source for a
list of partitions. For example, if a Hive data source has 20
partitions, each would be returned by the ConnectSplitManager
instance.

In the case of the example HTTP connect, each table only has a single
partition, and the ExampleSplitManager simply returns splits that
reflect this single partition reality of HTTP connector.

Record Set Provider: ExampleRecordSetProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The record set provider has a simple job. Given a split and a list of
columns: return a RecordSet object.  Note that the example HTTP
connector doesn't really split data up into multiple
partitions. Unlike Hive, when you are querying an HTTP data source at
a URL, it doesn't have the concept of splitting up the response into a
series of splits.

If you dig into the ExampleRecordSet and the ExampleRecordCursor
object you will see that the example HTTP connector understands
responses to be in a series of comma-separated fields on independent
lines.



