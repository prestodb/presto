============
SPI Overview
============

When you implement a new Presto plugin, you implement interfaces and
override methods defined by the SPI.

Plugins can provide additional connectors, types, and functions (see
:doc:`/develop/connector`, :doc:`/develop/types`, and :doc:`/develop/functions` respectively).
Connectors in particular are the source of all data for queries in
Presto: they back each catalog available to Presto.

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
to understand the Presto SPI. The ``getServices()`` method is a top-level
function that Presto calls to retrieve the relevant type: ``ConnectorFactory``
when Presto creates an instance of a connector to back a catalog, ``Type``
(or ``ParametricType``) when Presto creates types, and ``FunctionFactory``
when Presto creates functions.

Building Plugins via Maven
--------------------------

Plugins depend on the SPI from Presto:

.. code-block:: xml

    <dependency>
        <groupId>com.facebook.presto</groupId>
        <artifactId>presto-spi</artifactId>
        <scope>provided</scope>
    </dependency>

The plugin uses the Maven ``provided`` scope because Presto provides
the classes from the SPI at runtime and thus the plugin should not
include them in the plugin assembly.

There are a few other dependencies that are provided by Presto such
as ``javax.inject`` and Jackson. In particular, Jackson is used for
serializing handles and thus plugins must use the version provided
by Presto.

All other dependencies are based on what the plugin needs for its
own implementation. Plugins are loaded in a separate class loader
to provide isolation and to allow plugins to use a different version
of a library that Presto uses internally.

For an example ``pom.xml`` file, see the example HTTP connector in the
``presto-example-http`` directory in the root of the Presto source tree.

Adding a custom Plugin to Presto
--------------------------------
In order to add a custom plugin to Presto, create a directory for that
plugin in the Presto plugin directory. Then, add all the necessary jars
for the plugin to that directory. For example, for a plugin called
my-functions, you would create a directory my-functions in the Presto plugin
directory and add the relevant jars to that directory. By default, the plugin
directory is the ``plugin`` directory relative to the directory in which Presto
is installed, but it is configurable by the configuration variable
``plugin.config-dir``. In order for Presto to pick up the new plugin,
you must restart Presto.