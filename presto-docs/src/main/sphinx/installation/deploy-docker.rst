=================================
Deploy Presto From a Docker Image
=================================

These steps were developed and tested on Mac OS X, on both Intel and Apple Silicon chips. 

Follow these steps to:

- install the command line tools for brew, docker, and `Colima <https://github.com/abiosoft/colima>`_
- verify your Docker setup
- pull the Docker image of the Presto server
- start your local Presto server

Installing brew, Docker, and Colima
===================================

This task shows how to install brew, then to use brew to install Docker and Colima. 

Note: If you have Docker installed you can skip steps 1-3, but you should 
verify your Docker setup by running the command in step 4.

1. If you do not have brew installed, run the following command:

   ``/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"``

2. To install the Docker command line and `Colima <https://github.com/abiosoft/colima>`_ tools, run the following command:

   ``brew install docker colima``

3. Run the following command: 

   ``colima start``

   *Note*: The default VM created by Colima uses 2 CPUs, 2GB memory and 60GB storage. To customize the VM resources, 
   see the Colima README for `Customizing the VM <https://github.com/abiosoft/colima#customizing-the-vm>`_.

4. To verify your local setup, run the following command:

   ``docker run hello-world``

   If you see a response similar to the following, you are ready.

   ``Hello from Docker!`` 
   ``This message shows that your installation appears to be working correctly.``

Installing and Running the Presto Docker container
==================================================

1. Download the latest non-edge Presto container from `Presto on DockerHub <https://hub.docker.com/r/prestodb/presto/tags>`_. Run the following command: 

   ``docker pull prestodb/presto:latest``

   Downloading the container may take a few minutes. When the download completes, go on to the next step.

2. On your local system, create a file named ``config.properties`` containing the following text: 

   .. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    discovery-server.enabled=true
    discovery.uri=http://localhost:8080

3. On your local system, create a file named ``jvm.config`` containing the following text: 

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

   ``docker run -p 8080:8080 -it -v ./config.properties:/opt/presto-server/etc/config.properties -v ./jvm.config:/opt/presto-server/etc/jvm.config --name presto prestodb/presto:latest``

   This command assigns the name ``presto`` for the newly-created container that uses the downloaded image ``prestodb/presto:latest``.

   The Presto server logs startup information in the terminal window. Once you see a response similar to the following, the Presto server is running in the Docker container.

   ``======== SERVER STARTED ========``

Removing the Presto Docker container
====================================
To remove the Presto Docker container, run the following two commands: 

``docker stop presto``

``docker rm presto``

These commands return the name of the container ``presto`` when they succeed. 
