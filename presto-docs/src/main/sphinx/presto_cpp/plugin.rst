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

2. Set the ``plugin.dir`` property to the path of the ``plugins`` directory in the ``config.properties`` file of each of your workers.

3. Run the coordinator and workers to load your plugins.

