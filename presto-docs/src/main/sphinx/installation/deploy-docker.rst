=========================
Deploy Presto with Docker
=========================

This guide explains how to install and get started with Presto using Docker.

.. note::

   These steps were developed and tested on Mac OS X, on both Intel and Apple Silicon chips.

Prepare the container environment
=================================

If Docker is already installed, skip to step 4 to verify the setup.
Otherwise, follow the instructions below to install Docker and Colima using Homebrew or choose an alternative method.

1. Install `Homebrew <https://brew.sh/>`_ if it is not already present on the system.

2. Install the Docker command line and `Colima <https://github.com/abiosoft/colima>`_ tools via the following command:

   .. code-block:: shell

      brew install docker colima

3. Run the following command to start Colima with defaults:

   .. code-block:: shell

      colima start

   .. note::

      The default VM created by Colima uses 2 CPUs, 2GiB memory and 100GiB storage. To customize the VM resources,
      see the Colima README for `Customizing the VM <https://github.com/abiosoft/colima#customizing-the-vm>`_.

4. Verify the local setup by running the following command:

   .. code-block:: shell

      docker run hello-world

   The following output confirms a successful installation.

   .. code-block:: shell
      :class: no-copy

      Hello from Docker!
      This message shows that your installation appears to be working correctly.

Installing and Running the Presto Docker container
==================================================

1. Download the latest non-edge Presto container from `Presto on DockerHub <https://hub.docker.com/r/prestodb/presto/tags>`_:

   .. code-block:: shell

      docker pull prestodb/presto:latest

   Downloading the container may take a few minutes. When the download completes, go on to the next step.

2. On the local system, create a file named ``config.properties`` containing the following text:

   .. code-block:: properties

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    discovery-server.enabled=true
    discovery.uri=http://localhost:8080

3. On the local system, create a file named ``jvm.config`` containing the following text:

   .. code-block:: none

    -server
    -Xmx2G
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+UseGCOverheadLimit
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:+ExitOnOutOfMemoryError
    -Djdk.attach.allowAttachSelf=true
     
4. To start the Presto server in the Docker container, run the command:

   .. code-block:: shell

      docker run -p 8080:8080 -it -v ./config.properties:/opt/presto-server/etc/config.properties -v ./jvm.config:/opt/presto-server/etc/jvm.config --name presto prestodb/presto:latest

   This command assigns the name ``presto`` for the newly-created container that uses the downloaded image ``prestodb/presto:latest``.

   The Presto server logs startup information in the terminal window. The following output confirms the Presto server is running in the Docker container.

   .. code-block:: shell
      :class: no-copy

      ======== SERVER STARTED ========

Removing the Presto Docker container
====================================
To stop and remove the Presto Docker container, run the following commands:

.. code-block:: shell

   docker stop presto
   docker rm presto

These commands return the name of the container ``presto`` when they succeed. 
