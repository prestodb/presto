=======================
Presto C++ Installation
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

This shows how to install and run a lightweight Presto cluster utilizing a PrestoDB Java Coordinator and Prestissimo (Presto C++) Workers using Docker.

For more information about Presto C++, see the :ref:`presto-cpp:overview`.

The setup uses Meta's high-performance Velox engine for worker-side query execution to configure a cluster and run a test query with the built-in TPC-H connector.

Prerequisites
-------------

To follow this tutorial, you need:

* Docker installed.
* Basic familiarity with the terminal and shell commands.

Create a Working Directory
--------------------------
The recommended directory structure uses ``presto-lab`` as the root directory.

Create a clean root directory to hold all necessary configuration files and the ``docker-compose.yml`` file.

.. code-block:: bash

   mkdir -p ~/presto-lab
   cd ~/presto-lab

Configure the Presto Java Coordinator
-------------------------------------

The Coordinator requires configuration to define its role, enable the discovery service, and set up a catalog for querying.

1. Create Configuration Directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create the necessary directories for the coordinator and its catalogs, run the following command:

.. code-block:: bash

   mkdir -p coordinator/etc/catalog


2. Create the Coordinator Configuration File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create the file ``coordinator/etc/config.properties`` with the following contents. This file enables the coordinator mode, the discovery server, and sets the HTTP port to ``8080``.

.. code-block:: properties

   # coordinator/etc/config.properties
   coordinator=true
   node-scheduler.include-coordinator=true
   http-server.http.port=8080
   discovery-server.enabled=true
   discovery.uri=http://localhost:8080

* ``coordinator=true``: Enables the coordinator mode.
* ``discovery-server.enabled=true``: Designates the coordinator as the host for the worker discovery service.
* ``http-server.http.port=8080S``: Start the HTTP server on port 8080 for the coordinator (and workers, if enabled).

3. Create the JVM Configuration File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create the file ``coordinator/etc/jvm.config`` with the following content. These are standard Java 17 flags for Presto that ensures compatibility with Java 17's module system, provides stable garbage collection and memory behavior, and enforces safe failure handling.

.. code-block:: properties

   # coordinator/etc/jvm.config
   -server
   -Xmx1G
   -XX:+UseG1GC
   -XX:G1HeapRegionSize=32M
   -XX:+UseGCOverheadLimit
   -XX:+ExplicitGCInvokesConcurrent
   -XX:+HeapDumpOnOutOfMemoryError
   -XX:+ExitOnOutOfMemoryError
   -Djdk.attach.allowAttachSelf=true
   --add-opens=java.base/java.io=ALL-UNNAMED
   --add-opens=java.base/java.lang=ALL-UNNAMED
   --add-opens=java.base/java.lang.ref=ALL-UNNAMED
   --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
   --add-opens=java.base/java.net=ALL-UNNAMED
   --add-opens=java.base/java.nio=ALL-UNNAMED
   --add-opens=java.base/java.security=ALL-UNNAMED
   --add-opens=java.base/javax.security.auth=ALL-UNNAMED
   --add-opens=java.base/javax.security.auth.login=ALL-UNNAMED
   --add-opens=java.base/java.text=ALL-UNNAMED
   --add-opens=java.base/java.util=ALL-UNNAMED
   --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
   --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
   --add-opens=java.base/java.util.regex=ALL-UNNAMED
   --add-opens=java.base/jdk.internal.loader=ALL-UNNAMED
   --add-opens=java.base/sun.security.action=ALL-UNNAMED
   --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED

4. Create the Node Properties File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create the file ``coordinator/etc/node.properties`` with the following content to set the node environment and the data directory.

.. code-block:: properties

   # coordinator/etc/node.properties
   node.id=${ENV:HOSTNAME}
   node.environment=test
   node.data-dir=/var/lib/presto/data

5. Create the TPC-H Catalog Configuration File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create the file ``coordinator/etc/catalog/tpch.properties`` with the following content. The TPC-H catalog enables running test queries against an in-memory dataset.

.. code-block:: properties

   # coordinator/etc/catalog/tpch.properties
   connector.name=tpch

Configure the Prestissimo (C++) Worker
--------------------------------------

Configure the Worker to locate the Coordinator or Discovery service and identify itself within the network.

1. Create Worker Configuration Directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   mkdir -p worker-1/etc/catalog

2. Create ``worker-1/etc/config.properties``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Configure the worker to point to the discovery service running on the coordinator.

Note: You can repeat this step to add more workers, such as ``worker-2``.

.. code-block:: properties

   # worker-1/etc/config.properties
   discovery.uri=http://coordinator:8080
   presto.version=0.288-15f14bb
   http-server.http.port=7777
   shutdown-onset-sec=1
   runtime-metrics-collection-enabled=true

* ``discovery.uri=http://coordinator:8080``: This uses the coordinator service name as defined in the ``docker-compose.yml`` file for network communication within Docker.

3. Configure ``worker-1/etc/node.properties``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Define the worker’s internal address to ensure reliable registration.

.. code-block:: properties

   # worker-1/etc/node.properties
   node.environment=test
   node.internal-address=worker-1
   node.location=docker
   node.id=worker-1

* ``node.internal-address=worker-1``: This setting matches the service name defined in :ref:`Docker Compose <create-docker-compose-yml>`.

4. Add TPC-H Catalog Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Configure the worker with the same catalog definitions as the coordinator to execute query stages

.. code-block:: properties

   # worker-1/etc/catalog/tpch.properties
   connector.name=tpch

.. _create-docker-compose-yml:

Create ``docker-compose.yml``
-----------------------------

Create a ``docker-compose.yml`` file in the ``~/presto-lab`` directory to orchestrate both the Java Coordinator and the C++ Worker containers.

.. code-block:: yaml

   # docker-compose.yml
   services:
     coordinator:
       image: public.ecr.aws/oss-presto/presto:latest
       platform: linux/amd64
       container_name: presto-coordinator
       hostname: coordinator
       ports:
         - "8080:8080"
       volumes:
         - ./coordinator/etc:/opt/presto-server/etc:ro
       restart: unless-stopped

     worker-1:
       image: public.ecr.aws/oss-presto/presto-native:latest
       platform: linux/amd64
       container_name: prestissimo-worker-1
       hostname: worker-1
       depends_on:
         - coordinator
       volumes:
         - ./worker-1/etc:/opt/presto-server/etc:ro
       restart: unless-stopped

     worker-2:
       image: public.ecr.aws/oss-presto/presto-native:latest
       platform: linux/amd64
       container_name: prestissimo-worker-2
       hostname: worker-2
       depends_on:
         - coordinator
       volumes:
         - ./worker-2/etc:/opt/presto-server/etc:ro
       restart: unless-stopped

* The coordinator service uses the standard Java Presto image (presto:latest).
* The worker-1 and worker-2 services use the Prestissimo (C++ Native) image (presto-native:latest).
* The setting ``platform: linux/amd64`` is essential for users running on Apple Silicon Macs.
* The ``volumes`` section mounts your local configuration directories (``./coordinator/etc``, ``./worker-1/etc``) into the container's expected path (``/opt/presto-server/etc``).

Start the Cluster and Verify
----------------------------

1. Start the Cluster
^^^^^^^^^^^^^^^^^^^^

Use Docker Compose to start the cluster in detached mode (``-d``).

.. code-block:: bash

   docker compose up -d

2. Verify
^^^^^^^^^

1.  **Check the Web UI:** Open the Presto Web UI at http://localhost:8080.

    * You should see the UI displaying 3 Active Workers (1 Coordinator and 2 Workers).

2.  **Check Detailed Node Status** : Run the following SQL query to check the detailed status and metadata about every node (Coordinator and Workers).

    .. code-block:: sql

       select * from system.runtime.nodes;

    This confirms the cluster nodes are registered and active.