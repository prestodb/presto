=================================
Deploy Presto From a Docker Image
=================================

These steps were developed and tested on Mac OS X, on both Intel and Apple Silicon chips. 

Following these steps, you will:

- install the command line tools for brew, docker, and `colima <https://github.com/abiosoft/colima>`_ tools
- verify your Docker setup
- pull the Docker image of the Presto server and the Presto CLI
- start your local Presto server and Presto CLI
- query your local Presto server using the Presto CLI
- query a remote Presto server using the Presto CLI

Installing brew, docker, and colima
===================================================

This task shows how to install brew, then to use brew to install docker and colima. 

Note: If you have Docker installed you can skip steps 1-3. It is recommended that you 
verify your Docker setup by running the command in step 4.

1. If you do not have brew installed, run the following command:

   ``/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"``

2. To install the docker command line and `colima <https://github.com/abiosoft/colima>`_ tools, run the following command:

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

1. Run the following command to download the prestodb-sandbox container from `DockerHub <https://hub.docker.com/r/ahanaio/prestodb-sandbox>`_: 

   ``docker pull ahanaio/prestodb-sandbox``

   Downloading the container may take a few minutes. When the download completes, go on to the next step.

2. To start the Presto server in the Docker container, run the following command:

   ``docker run -p 8080:8080 --name presto ahanaio/prestodb-sandbox``

   This command also assigns the name ``presto`` for ``ahanaio/prestodb-sandbox``.

   The Presto server begins logging startup information in the terminal window. Once you see a response similar to the following, the Presto server is running in the Docker container.

   ``======== SERVER STARTED ========``

3. To start the Presto CLI (command-line interface), open a new terminal window and run the following command:

   ``docker exec -it presto presto-cli``

   The presto prompt is shown:

   ``presto>``

   If the ``docker exec`` command returns an error similar to the following:
   
   ::
    
    "docker exec" requires at least 2 arguments.
    See 'docker exec --help'.
    Usage:  docker exec [OPTIONS] CONTAINER COMMAND [ARG...]
    Execute a command in a running container``

   the Presto server has not completed starting. Wait a little and try again, until the ``presto>`` prompt is shown.

4. To verify that your Presto server and Presto CLI are communicating together, use the Presto CLI to query your local Presto server. Run the following command at the ``presto>`` prompt:

   ``show catalogs;``

   The response should be similar to the following:

   ::

    presto> show catalogs;
    Catalog
    ---------
    jmx
    memory
    system
    tpcds
    tpch
    (5 rows)
    
    Query 20230614_181140_00000_uutw6, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    [Latency: client-side: 0:13, server-side: 0:11] [0 rows, 0B] [0 rows/s, 0B/s]
    
    presto>

5. To use the Presto CLI in your local Docker image to connect to a remote Presto server, run the following command: 

   ``docker exec -it presto presto-cli --server Presto-endpoint-URL --user username --password``

   You will be prompted to enter the password for the Presto user, then the ``presto>`` prompt is shown.

6. To verify your Presto server and Presto CLI are communicating together, use the Presto CLI to query your remote Presto server. 

   Run the ``show catalogs;`` command at the ``presto>`` prompt and the result should be similar to step 4.

Removing the Presto Docker container
====================================
When you no longer want the Presto Docker container, run the following two commands: 

``docker stop presto``

``docker rm presto``

The commands return the name of the container when they are successful. 
