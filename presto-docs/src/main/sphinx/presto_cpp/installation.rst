=======================
Presto C++ Installation
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

This shows how to install and run a lightweight Presto cluster utilizing a PrestoDB Java Coordinator and Prestissimo (C++) Workers using Docker.

The setup uses Meta's high-performance Velox engine for worker-side query execution. We will configure a cluster and run a test query with the built-in TPCH connector.

Introducing Prestissimo (Presto C++ Worker)
-------------------------------------------

Prestissimo is the C++ native implementation of the Presto :ref:`overview/concepts:worker`. It is designed to be a drop-in replacement for the traditional Java worker. It is built using Velox, a high-performance, open-source C++ database acceleration library created by Meta.

A C++ execution engine offers significant advantages for data lake analytics:

* **Massive Performance Boost:** Prestissimo achieves increases in CPU efficiency and reduces query latency by leveraging native C++ execution, vectorization, and SIMD (Single Instruction, Multiple Data) instructions.
* **Eliminates Java Garbage Collection Issues:** By moving the execution engine out of the Java Virtual Machine (JVM), this architecture removes performance spikes and pauses associated with Java Garbage Collection, resulting in more consistent and stable query times.
* **Explicit Memory Control:** The Velox memory management framework offers explicit memory accounting and arbitration, providing finer control over resource consumption than in the JVM.

Prerequisites
-------------

To follow this tutorial, you need:

* Docker installed.
* Basic familiarity with the terminal and shell commands.

Setup Guide
-----------

The recommended directory structure uses ``presto-lab`` as the root directory.

Create a Working Directory
^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a clean root directory to hold all necessary configuration files and the ``docker-compose.yml`` file.

.. code-block:: bash

   mkdir -p ~/presto-lab
   cd ~/presto-lab

Configure the Presto Java Coordinator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Coordinator requires configuration to define its role, enable the discovery service, and set up a catalog for querying.

A. Create Configuration Directory
"""""""""""""""""""""""""""""""""

.. code-block:: bash

   mkdir -p coordinator/etc/catalog

This command creates the necessary directories for the coordinator and its catalogs.

B. Create ``coordinator/etc/config.properties``
"""""""""""""""""""""""""""""""""""""""""""""""

This file enables the coordinator mode, the discovery server, and sets the HTTP port to ``8080``.

.. code-block:: properties

   # coordinator/etc/config.properties
   coordinator=true
   node-scheduler.include-coordinator=true
   http-server.http.port=8080
   discovery-server.enabled=true
   discovery.uri=http://localhost:8080

* ``coordinator=true``: Enables the coordinator mode.
* ``discovery-server.enabled=true``: Designates the coordinator as the host for the worker discovery service.

C. Create ``coordinator/etc/jvm.config``
""""""""""""""""""""""""""""""""""""""""

These are standard **Java 17** flags for Presto, optimizing the JVM.

.. code-block:: text

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

D. Create ``coordinator/etc/node.properties``
"""""""""""""""""""""""""""""""""""""""""""""

This file sets the node environment and the data directory.

.. code-block:: properties

   # coordinator/etc/node.properties
   node.id=${ENV:HOSTNAME}
   node.environment=test
   node.data-dir=/var/lib/presto/data

E. Add TPCH Catalog Configuration
"""""""""""""""""""""""""""""""""

The TPCH catalog enables running test queries against an in-memory dataset.

.. code-block:: properties

   # coordinator/etc/catalog/tpch.properties
   connector.name=tpch

Configure the Prestissimo (C++) Worker
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Worker must be configured to locate the Coordinator or Discovery service and identify itself within the network.

Repeat this step to add more workers, such as ``worker-2``.

A. Create Worker Configuration Directory
""""""""""""""""""""""""""""""""""""""""

.. code-block:: bash

   mkdir -p worker-1/etc/catalog

B. Create ``worker-1/etc/config.properties``
""""""""""""""""""""""""""""""""""""""""""""

This configuration points the worker to the discovery service running on the coordinator.

.. code-block:: properties

   # worker-1/etc/config.properties
   discovery.uri=http://coordinator:8080
   presto.version=0.288-15f14bb
   http-server.http.port=7777
   shutdown-onset-sec=1
   runtime-metrics-collection-enabled=true

* ``discovery.uri=http://coordinator:8080``: This uses the coordinator service name as defined in the ``docker-compose.yml`` file for network communication within Docker.

C. Configure ``worker-1/etc/node.properties``
"""""""""""""""""""""""""""""""""""""""""""""

This defines the worker's internal address for reliable registration.

.. code-block:: properties

   # worker-1/etc/node.properties
   node.environment=test
   node.internal-address=worker-1
   node.location=docker
   node.id=worker-1

* ``node.internal-address=worker-1``: This setting matches the service name defined in Docker Compose.

D. Add TPCH Catalog Configuration
"""""""""""""""""""""""""""""""""

The worker requires the same catalog definition as the coordinator to execute the query stages.

.. code-block:: properties

   # worker-1/etc/catalog/tpch.properties
   connector.name=tpch

Step 4: Create ``docker-compose.yml``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This file orchestrates both the Java Coordinator and the C++ Worker containers. Create the file ``docker-compose.yml`` in your ``~/presto-lab`` directory.

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

* The **coordinator** service uses the standard **Java Presto image** (presto:latest).
* The **worker-1** and **worker-2** services use the **Prestissimo (C++ Native) image** (presto-native:latest).
* The setting ``platform: linux/amd64`` is essential for users running on Apple Silicon Macs.
* The ``volumes`` section mounts your local configuration directories (``./coordinator/etc``, ``./worker-1/etc``) into the container's expected path (``/opt/presto-server/etc``).

Step 5: Start the Cluster and Verify
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A. Start the Cluster
""""""""""""""""""""

Use Docker Compose to start the cluster in detached mode (``-d``).

.. code-block:: bash

   docker compose up -d

B. Verify
"""""""""

1.  **Check the Web UI:** Open the Presto Web UI at http://localhost:8080.

    * *Verification Result:* You should see the UI displaying 3 Active Workers (1 Coordinator and 2 Workers).

2.  **Check Detailed Node Status (SQL Query):** Run the following query to check the detailed status and metadata about every node (Coordinator and Workers).

    .. code-block:: sql

       select * from system.runtime.nodes;

    This confirms the cluster nodes are registered and active.