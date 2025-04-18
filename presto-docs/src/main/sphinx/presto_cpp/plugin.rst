*******************
Presto C++ Plugins
*******************

This page lists the plugins in Presto C++ that are available for various use cases such as to load User Defined Functions (UDFs), connectors, or types, and describes the setup needed to use these plugins.

.. toctree::
    :maxdepth: 1

    plugin/function_plugin


Setup
-----

1. Place the plugin shared libraries in the ``plugins`` directory.

2. Create a Json configuration file to capture information on the shared libraries you wish to load dynamically. 

3. For each worker, edit the config.properties file:

    * Set the ``plugin.dir`` property to the path of the ``plugins`` directory.
    
    * Set the ``plugin.config`` property to the path of the Json configuration file.

4. Start or restart the coordinator and workers to pick up any placed libraries.

Note: To avoid issues with ABI compatibility, it is strongly recommended to recompile all shared library plugins during OS and Presto version upgrades.