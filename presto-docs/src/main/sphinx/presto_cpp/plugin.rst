******************
Presto C++ Plugins
******************

This page lists the plugins in Presto C++ that are available for various use cases such as to load User Defined Functions (UDFs) and describes the setup needed to use these plugins.

.. toctree::
    :maxdepth: 1

    plugin/function_plugin


Setup
-----

1. Place the plugin shared libraries in the ``plugin`` directory.

2. For each worker, edit the ``config.properties`` file to set the ``plugin.dir`` property to the path of the ``plugin`` directory.

   If ``plugin.dir`` is not specified, the path to this directory defaults to the ``plugin`` directory relative to the directory in which the process is being run.

3. Start or restart the coordinator and workers to pick up any placed libraries.

Note: To avoid issues with ABI compatibility, it is strongly recommended to recompile all shared library plugins during OS and Presto version upgrades.