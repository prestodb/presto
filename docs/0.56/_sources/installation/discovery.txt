=================
Discovery Service
=================

Installing Discovery
--------------------

Presto uses the
`Discovery <https://github.com/airlift/discovery>`_
service to find all the nodes in the cluster. Every Presto instance
will register itself with the Discovery service on startup.

Discovery is configured and run the same way as Presto. Download
`discovery-server-1.16.tar.gz`_, unpack it to create the *installation*
directory, create the *data* directory, then configure it to run on a
different port than Presto. The standard port for Discovery is ``8411``.

.. _discovery-server-1.16.tar.gz: http://central.maven.org/maven2/io/airlift/discovery/discovery-server/1.16/discovery-server-1.16.tar.gz

Configuring Discovery
---------------------

As with Presto, create an ``etc`` directory inside the installation
directory to hold the configuration files.

Node Properties
^^^^^^^^^^^^^^^

Create the :ref:`presto_node_properties` file the same way as for Presto,
but make sure to use a unique value for ``node.id``. For example:

.. code-block:: none

    node.environment=production
    node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
    node.data-dir=/var/discovery/data

JVM Config
^^^^^^^^^^

Create the :ref:`presto_jvm_config` file the same way as for Presto, but
configure it to use fewer resources:

.. code-block:: none

    -server
    -Xmx1G
    -XX:+UseConcMarkSweepGC
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+AggressiveOpts
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:OnOutOfMemoryError=kill -9 %p

Config Properties
^^^^^^^^^^^^^^^^^

Create ``etc/config.properties`` with the following lone option:

.. code-block:: none

    http-server.http.port=8411

Running Discovery
-----------------

Discovery is run the same way as Presto.
See :ref:`running_presto` for details.
