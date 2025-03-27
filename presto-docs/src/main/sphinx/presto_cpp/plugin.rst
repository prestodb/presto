*******************
Presto C++ Plugins
*******************

This chapter outlines the plugins in Presto C++ that are available for various use cases such as to load User Defined Functions (UDFs), connectors, or types.

.. toctree::
    :maxdepth: 1

    plugin/function_plugin


Setup
-----

1. Place the plugin shared libraries in the ``plugins`` directory.

2. Create a Json configuration file where you will capture information on the shared libraries you wish to load dynamically. 

3. Set the ``plugin.dir`` property to the path of the ``plugins`` directory in the ``config.properties`` file of each of your workers.

4. Set the ``plugin.config`` property to the path of the Json configuration file of each of your workers.

5. Start or restart the coordinator and workers to pick up any placed libraries.

Note: to avoid issues with ABI compatibility, we strongly recommend recompiling all shared library plugins during OS and presto version upgrades.