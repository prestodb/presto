============
SPI Overview
============

When you implement a new Presto plugin, you implement interfaces and
override methods defined by the SPI.

Plugins can provide additional :doc:`connectors`, :doc:`types`,
:doc:`functions` and :doc:`system-access-control`.
In particular, connectors are the source of all data for queries in
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

For a built-in plugin that is included in the Presto source code,
this resource file is created whenever the ``pom.xml`` file of a plugin
contains the following line:

.. code-block:: none

    <packaging>presto-plugin</packaging>

Plugin
------

The ``Plugin`` interface is a good starting place for developers looking
to understand the Presto SPI. It contains access methods to retrieve
various classes that a Plugin can provide. For example, the ``getConnectorFactories()``
method is a top-level function that Presto calls to retrieve a ``ConnectorFactory`` when Presto
is ready to create an instance of a connector to back a catalog. There are similar
methods for ``Type``, ``ParametricType``, ``Function``, ``SystemAccessControl``, and
``EventListenerFactory`` objects.

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

There are a few other dependencies that are provided by Presto,
including Slice and Jackson annotations. In particular, Jackson is
used for serializing connector handles and thus plugins must use the
annotations version provided by Presto.

All other dependencies are based on what the plugin needs for its
own implementation. Plugins are loaded in a separate class loader
to provide isolation and to allow plugins to use a different version
of a library that Presto uses internally.

For an example ``pom.xml`` file, see the example HTTP connector in the
``presto-example-http`` directory in the root of the Presto source tree.

Deploying a Custom Plugin
-------------------------

In order to add a custom plugin to a Presto installation, create a directory
for that plugin in the Presto plugin directory and add all the necessary jars
for the plugin to that directory. For example, for a plugin called
``my-functions``, you would create a directory ``my-functions`` in the Presto
plugin directory and add the relevant jars to that directory.

By default, the plugin directory is the ``plugin`` directory relative to the
directory in which Presto is installed, but it is configurable using the
configuration variable ``catalog.config-dir``. In order for Presto to pick up
the new plugin, you must restart Presto.

Plugins must be installed on all nodes in the Presto cluster (coordinator and workers).

Coordinator Plugin
------------------

The ``CoordinatorPlugin`` interface allows plugins to provide additional
functionality for the Presto coordinator. Presto SPI defines different service
provider factories and service providers that allow customization of session
property providers, function namespace managers, type managers, expression
optimizers, and plan checkers. The following service providers can be accessed
via their respective provider factories.

+----------------------+----------------------------------------+---------------------------------+
|       Service        |             Provider Factory           |        Service Provider         |
+======================+========================================+=================================+
|  Session Properties  |  WorkerSessionPropertyProviderFactory  |  WorkerSessionPropertyProvider  |
+----------------------+----------------------------------------+---------------------------------+
|      Functions       |     FunctionNamespaceManagerFactory    |    FunctionNamespaceManager     |
+----------------------+----------------------------------------+---------------------------------+
|        Types         |           TypeManagerFactory           |           TypeManager           |
+----------------------+----------------------------------------+---------------------------------+
| Expression Optimizer |       ExpressionOptimizerFactory       |       ExpressionOptimizer       |
+----------------------+----------------------------------------+---------------------------------+
|    Plan Checker      |       PlanCheckerProviderFactory       |       PlanCheckerProvider       |
+----------------------+----------------------------------------+---------------------------------+

``CoordinatorPlugin`` interface provides methods to access all registered
provider factories that customize these services. In a Presto C++ cluster,
the class ``NativeSidecarPlugin`` implements ``CoordinatorPlugin`` interface
to customize functionality for Presto C++.

.. _native-sidecar-plugin:

Native Sidecar Plugin
---------------------

The ``NativeSidecarPlugin`` class implements ``CoordinatorPlugin`` interface
and returns the following service providers via their respective provider
factories.

+----------------------+----------------------------------------------+---------------------------------------+
|       Service        |           Native Provider Factory            |        Native Service Provider        |
+======================+==============================================+=======================================+
|  Session Properties  |  NativeSystemSessionPropertyProviderFactory  |  NativeSystemSessionPropertyProvider  |
+----------------------+----------------------------------------------+---------------------------------------+
|      Functions       |     NativeFunctionNamespaceManagerFactory    |    NativeFunctionNamespaceManager     |
+----------------------+----------------------------------------------+---------------------------------------+
|        Types         |           NativeTypeManagerFactory           |           NativeTypeManager           |
+----------------------+----------------------------------------------+---------------------------------------+
|    Plan Checker      |       NativePlanCheckerProviderFactory       |       NativePlanCheckerProvider       |
+----------------------+----------------------------------------------+---------------------------------------+

For instance, the class ``NativeSystemSessionPropertyProviderFactory``
implements the interface ``WorkerSessionPropertyProviderFactory`` in Presto SPI
to return the service provider, ``NativeSystemSessionPropertyProvider``. The class
``NativeSystemSessionPropertyProvider`` retrieves all session properties
supported by the Presto C++ worker by making a REST call to the endpoint
``/v1/properties/session`` on the Presto C++ sidecar. ``NativeSidecarPlugin``,
therefore, needs at least one Presto C++ worker in the cluster to be configured
as a sidecar. See :doc:`/presto_cpp/sidecar` for more details.
