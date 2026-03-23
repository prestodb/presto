=========================
Deploying Custom Plugins
=========================

Presto has a plugin-based architecture. Connectors, functions, access control
implementations, and other features are provided by plugins that are loaded at
server startup. Many plugins are bundled with a Presto installation, but
additional plugins can be deployed manually.

This topic explains how to deploy a custom or additional plugin to a Presto
installation.

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

When Presto starts, it scans the plugin directory for subdirectories. Each
subdirectory is treated as a separate plugin and must contain all of the
Java Archive (JAR) files required by that plugin. Presto loads each plugin
into a separate class loader to ensure that plugins are isolated from each
other.

If a catalog configuration file (in ``etc/catalog``) references a connector
whose corresponding plugin has not been deployed, Presto will fail to start.

Plugin Directory
----------------

By default, the plugin directory is the ``plugin`` directory relative to the
Presto installation directory. This can be changed using the ``plugin.dir``
configuration property.

The plugin directory is structured as follows, where each subdirectory
contains the JAR files for a single plugin:

.. code-block:: none

    plugin/
    ├── hive-hadoop2/
    │   ├── presto-hive-hadoop2-0.296.jar
    │   └── ... (dependency JARs)
    ├── tpch/
    │   ├── presto-tpch-0.296.jar
    │   └── ... (dependency JARs)
    └── gsheets/
        ├── presto-google-sheets-0.296.jar
        └── ... (dependency JARs)

Deploying a Plugin
------------------

To deploy a plugin:

1. Obtain the plugin directory for the plugin you want to deploy. This
   directory should contain the plugin JAR file and all of its required
   dependencies. Plugin directories can be obtained by building the plugin
   from source or from a Presto distribution that includes the plugin.

2. Copy the plugin directory into the Presto plugin directory. The name of the
   subdirectory does not need to match the connector name, but using a matching
   name is recommended for clarity.

   For example, to deploy the Google Sheets connector plugin on a Presto
   installation located at ``/opt/presto-server``:

   .. code-block:: none

       cp -r presto-google-sheets-0.296/ /opt/presto-server/plugin/gsheets

3. Deploy the plugin on all nodes in the Presto cluster (coordinator and
   workers). Every node must have the same set of plugins installed.

4. Restart Presto. A plugin is loaded only at server startup.

Verifying a Plugin
------------------

After restarting Presto, check the server log file (``var/log/server.log``)
for entries showing that the plugin was loaded and its connector was
registered. For example:

.. code-block:: none

    INFO  main  com.facebook.presto.server.PluginManagerUtil  -- Loading plugin /opt/presto-server/plugin/gsheets --
    INFO  main  com.facebook.presto.server.PluginManagerUtil  Installing com.facebook.presto.google.sheets.SheetsPlugin
    INFO  main  com.facebook.presto.server.PluginManager      Registering connector gsheets
    INFO  main  com.facebook.presto.server.PluginManagerUtil  -- Finished loading plugin /opt/presto-server/plugin/gsheets --

If the server log shows ``======== SERVER STARTED ========``, the plugin was
loaded successfully.

Troubleshooting
---------------

**Server fails to start with "No factory for connector"**

.. code-block:: none

    ERROR  main  com.facebook.presto.server.PrestoServer  No factory for connector gsheets
    java.lang.IllegalArgumentException: No factory for connector gsheets

This error indicates that the connector plugin has not been deployed. Verify
that:

* The plugin directory exists in the Presto plugin directory.
* The plugin directory contains the required JAR files.
* Presto has been restarted after deploying the plugin.

**Plugin directory is empty or contains incorrect JARs**

Each plugin directory must contain the plugin JAR file and all of its
transitive dependencies. If a required JAR file is missing, the plugin may
fail to load or the connector may produce errors at runtime.

See Also
--------

* :doc:`deployment` -- Deploying Presto
* :doc:`/develop/spi-overview` -- SPI Overview, including information about
  building plugins from source
