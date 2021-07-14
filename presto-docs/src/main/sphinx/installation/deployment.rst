================
Deploying Presto
================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Installing Presto
-----------------

Download the Presto server tarball, :maven_download:`server`, and unpack it.
The tarball will contain a single top-level directory,
|presto_server_release|, which we will call the *installation* directory.

Presto needs a *data* directory for storing logs, etc.
We recommend creating a data directory outside of the installation directory,
which allows it to be easily preserved when upgrading Presto.

Configuring Presto
------------------

Create an ``etc`` directory inside the installation directory.
This will hold the following configuration:

* Node Properties: environmental configuration specific to each node
* JVM Config: command line options for the Java Virtual Machine
* Config Properties: configuration for the Presto server
* Catalog Properties: configuration for :doc:`/connector` (data sources)

.. _presto_node_properties:

Node Properties
^^^^^^^^^^^^^^^

The node properties file, ``etc/node.properties``, contains configuration
specific to each node. A *node* is a single installed instance of Presto
on a machine. This file is typically created by the deployment system when
Presto is first installed. The following is a minimal ``etc/node.properties``:

.. code-block:: none

    node.environment=production
    node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
    node.data-dir=/var/presto/data

The above properties are described below:

* ``node.environment``:
  The name of the environment. All Presto nodes in a cluster must
  have the same environment name.

* ``node.id``:
  The unique identifier for this installation of Presto. This must be
  unique for every node. This identifier should remain consistent across
  reboots or upgrades of Presto. If running multiple installations of
  Presto on a single machine (i.e. multiple nodes on the same machine),
  each installation must have a unique identifier.

* ``node.data-dir``:
  The location (filesystem path) of the data directory. Presto will store
  logs and other data here.

.. _presto_jvm_config:

JVM Config
^^^^^^^^^^

The JVM config file, ``etc/jvm.config``, contains a list of command line
options used for launching the Java Virtual Machine. The format of the file
is a list of options, one per line. These options are not interpreted by
the shell, so options containing spaces or other special characters should
not be quoted.

The following provides a good starting point for creating ``etc/jvm.config``:

.. code-block:: none

    -server
    -Xmx16G
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+UseGCOverheadLimit
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:+ExitOnOutOfMemoryError

Because an ``OutOfMemoryError`` will typically leave the JVM in an
inconsistent state, we write a heap dump (for debugging) and forcibly
terminate the process when this occurs.


.. _config_properties:

Config Properties
^^^^^^^^^^^^^^^^^

The config properties file, ``etc/config.properties``, contains the
configuration for the Presto server. Every Presto server can function
as both a coordinator and a worker, but dedicating a single machine
to only perform coordination work provides the best performance on
larger clusters.

The following is a minimal configuration for the coordinator:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=false
    http-server.http.port=8080
    query.max-memory=50GB
    query.max-memory-per-node=1GB
    query.max-total-memory-per-node=2GB
    discovery-server.enabled=true
    discovery.uri=http://example.net:8080

And this is a minimal configuration for the workers:

.. code-block:: none

    coordinator=false
    http-server.http.port=8080
    query.max-memory=50GB
    query.max-memory-per-node=1GB
    query.max-total-memory-per-node=2GB
    discovery.uri=http://example.net:8080

Alternatively, if you are setting up a single machine for testing that
will function as both a coordinator and worker, use this configuration:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    query.max-memory=5GB
    query.max-memory-per-node=1GB
    query.max-total-memory-per-node=2GB
    discovery-server.enabled=true
    discovery.uri=http://example.net:8080

These properties require some explanation:

* ``coordinator``:
  Allow this Presto instance to function as a coordinator
  (accept queries from clients and manage query execution).

* ``node-scheduler.include-coordinator``:
  Allow scheduling work on the coordinator.
  For larger clusters, processing work on the coordinator
  can impact query performance because the machine's resources are not
  available for the critical task of scheduling, managing and monitoring
  query execution.

* ``http-server.http.port``:
  Specifies the port for the HTTP server. Presto uses HTTP for all
  communication, internal and external.

* ``query.max-memory``:
  The maximum amount of distributed memory that a query may use.

* ``query.max-memory-per-node``:
  The maximum amount of user memory that a query may use on any one machine.

* ``query.max-total-memory-per-node``:
  The maximum amount of user and system memory that a query may use on any one machine,
  where system memory is the memory used during execution by readers, writers, and network buffers, etc.

* ``discovery-server.enabled``:
  Presto uses the Discovery service to find all the nodes in the cluster.
  Every Presto instance will register itself with the Discovery service
  on startup. In order to simplify deployment and avoid running an additional
  service, the Presto coordinator can run an embedded version of the
  Discovery service. It shares the HTTP server with Presto and thus uses
  the same port.

* ``discovery.uri``:
  The URI to the Discovery server. Because we have enabled the embedded
  version of Discovery in the Presto coordinator, this should be the
  URI of the Presto coordinator. Replace ``example.net:8080`` to match
  the host and port of the Presto coordinator. This URI must not end
  in a slash.

You may also wish to set the following properties:

* ``jmx.rmiregistry.port``:
  Specifies the port for the JMX RMI registry. JMX clients should connect to this port.

* ``jmx.rmiserver.port``:
  Specifies the port for the JMX RMI server. Presto exports many metrics
  that are useful for monitoring via JMX.

See also :doc:`/admin/resource-groups`.

Log Levels
^^^^^^^^^^

The optional log levels file, ``etc/log.properties``, allows setting the
minimum log level for named logger hierarchies. Every logger has a name,
which is typically the fully qualified name of the class that uses the logger.
Loggers have a hierarchy based on the dots in the name (like Java packages).
For example, consider the following log levels file:

.. code-block:: none

    com.facebook.presto=INFO

This would set the minimum level to ``INFO`` for both
``com.facebook.presto.server`` and ``com.facebook.presto.hive``.
The default minimum level is ``INFO``
(thus the above example does not actually change anything).
There are four levels: ``DEBUG``, ``INFO``, ``WARN`` and ``ERROR``.

.. _catalog_properties:

Catalog Properties
^^^^^^^^^^^^^^^^^^

Presto accesses data via *connectors*, which are mounted in catalogs.
The connector provides all of the schemas and tables inside of the catalog.
For example, the Hive connector maps each Hive database to a schema,
so if the Hive connector is mounted as the ``hive`` catalog, and Hive
contains a table ``clicks`` in database ``web``, that table would be accessed
in Presto as ``hive.web.clicks``.

Catalogs are registered by creating a catalog properties file
in the ``etc/catalog`` directory.
For example, create ``etc/catalog/jmx.properties`` with the following
contents to mount the ``jmx`` connector as the ``jmx`` catalog:

.. code-block:: none

    connector.name=jmx

See :doc:`/connector` for more information about configuring connectors.

.. _running_presto:

Running Presto
--------------

The installation directory contains the launcher script in ``bin/launcher``.
Presto can be started as a daemon by running the following:

.. code-block:: none

    bin/launcher start

Alternatively, it can be run in the foreground, with the logs and other
output being written to stdout/stderr (both streams should be captured
if using a supervision system like daemontools):

.. code-block:: none

    bin/launcher run

Run the launcher with ``--help`` to see the supported commands and
command line options. In particular, the ``--verbose`` option is
very useful for debugging the installation.

After launching, you can find the log files in ``var/log``:

* ``launcher.log``:
  This log is created by the launcher and is connected to the stdout
  and stderr streams of the server. It will contain a few log messages
  that occur while the server logging is being initialized and any
  errors or diagnostics produced by the JVM.

* ``server.log``:
  This is the main log file used by Presto. It will typically contain
  the relevant information if the server fails during initialization.
  It is automatically rotated and compressed.

* ``http-request.log``:
  This is the HTTP request log which contains every HTTP request
  received by the server. It is automatically rotated and compressed.

An Example Deployment on Laptop Querying S3
-------------------------------------------

This section shows how to run Presto connecting to Hive MetaStore on a single laptop to query data in an S3 bucket.

Configure Hive MetaStore
^^^^^^^^^^^^^^^^^^^^^^^^

Download and extract the binary tarball of Hive.
For example, download and untar `apache-hive-<VERSION>-bin.tar.gz <https://downloads.apache.org/hive>`_ .

You only need to launch Hive Metastore to serve Presto catalog information such as table schema and partition location.
If it is the first time to launch the Hive Metastore, prepare corresponding configuration files and environment, also initialize a new Metastore:

.. code-block:: console

    export HIVE_HOME=`pwd`
    cp conf/hive-default.xml.template conf/hive-site.xml
    mkdir -p hcatalog/var/log/
    bin/schematool -dbType derby -initSchema

If you want to access AWS S3, append the following lines in ``conf/hive-env.sh``.
Hive needs the corresponding jars to access files with ``s3a://`` addresses, and AWS credentials as well to access an S3 bucket (even it is public).
These jars can be found in Hadoop distribution (e.g., under ``${HADOOP_HOME}/share/hadoop/tools/lib/``),
or download from `maven central repository <https://repo1.maven.org/>`_.

.. code-block:: bash

    export HIVE_AUX_JARS_PATH=/path/to/aws-java-sdk-core-<version>.jar:$/path/to/aws-java-sdk-s3-<version>.jar:/path/to/hadoop-aws-<version>.jar
    export AWS_ACCESS_KEY_ID=<Your AWS Access Key>
    export AWS_SECRET_ACCESS_KEY=<Your AWS Secret Key>

Start a Hive Metastore which will run in the background and listen on port 9083 (by default):

.. code-block:: console

    hcatalog/sbin/hcat_server.sh start

The output is similar to the following:

.. code-block:: console

    Started metastore server init, testing if initialized correctly...
    Metastore initialized successfully on port[9083].

To verify if the MetaStore is running, check the Hive Metastore logs at ``hcatalog/var/log/``

Configure Presto
^^^^^^^^^^^^^^^^

Create a configuration file ``etc/config.properties`` to based on `Config Properties <#config-properties>`_.
For example, follow the minimal configuration to run Presto on your laptop:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    discovery-server.enabled=true
    discovery.uri=http://localhost:8080

Create ``etc/jvm.config`` according to `JVM Config <#jvm-config>`_
and ``etc/node.properties`` according to `Node Properties <#node-properties>`_.

Lastly, configure Presto Hive connector in ``etc/catalog/hive.properties``, pointing to the Hive Metastore service just started.
Include AWS credentials here again if Presto needs to read input files from S3.

.. code-block:: none

    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://localhost:9083
    hive.s3.aws-access-key=<Your AWS Access Key>
    hive.s3.aws-secret-key=<Your AWS Secret Key>

Run the Presto server:

.. code-block:: bash

    ./bin/launcher start


An Example Deployment with Docker
---------------------------------

Let's take a look at getting a Docker image together for Presto (though they already exist on Dockerhub,
e.g. `ahanaio/prestodb-sandbox <https://hub.docker.com/r/ahanaio/prestodb-sandbox>`_).
We can see below how relatively easy it is to get Presto up and running.
For demonstration purposes, this configuration is a single-node Presto installation where the scheduler will include the Coordinator as a Worker.
We will configure one catalog, `TPCH <https://prestodb.io/docs/current/connector/tpch.html>`_.

For the Dockerfile, we download Presto, copy some configuration files in a local ``etc`` directory into the image,
and specify an entry point to run the server.

.. code-block:: docker

    FROM openjdk:8-jre

    # Presto version will be passed in at build time
    ARG PRESTO_VERSION

    # Set the URL to download
    ARG PRESTO_BIN=https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz

    # Update the base image OS and install wget and python
    RUN apt-get update
    RUN apt-get install -y wget python less

    # Download Presto and unpack it to /opt/presto
    RUN wget --quiet ${PRESTO_BIN}
    RUN mkdir -p /opt
    RUN tar -xf presto-server-${PRESTO_VERSION}.tar.gz -C /opt
    RUN rm presto-server-${PRESTO_VERSION}.tar.gz
    RUN ln -s /opt/presto-server-${PRESTO_VERSION} /opt/presto

    # Copy configuration files on the host into the image
    COPY etc /opt/presto/etc

    # Download the Presto CLI and put it in the image
    RUN wget --quiet https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar
    RUN mv presto-cli-${PRESTO_VERSION}-executable.jar /usr/local/bin/presto
    RUN chmod +x /usr/local/bin/presto

    # Specify the entrypoint to start
    ENTRYPOINT /opt/presto/bin/launcher run

There are four files in the ``etc/`` folder to configure Presto, along with one catalog in ``etc/catalog/``. A catalog defines the configuration
of a connector, and the catalog is named after the file name (minus the ``.properties`` extension). You can have multiple
catalogs for each Presto installation, including multiple catalogs using the same connector; they just need a different filename.
The files are:

.. code-block:: none

    etc/
    ├── catalog
    │   └── tpch.properties  # Configures the TPCH connector to generate data
    ├── config.properties    # Presto instance configuration properties
    ├── jvm.config           # JVM configuration for the process
    ├── log.properties       # Logging configuration
    └── node.properties      # Node-specific configuration properties

The four files directly under ``etc`` are documented above (using the single-node Coordinator configuration for ``config.properties``).
The file called ``etc/catalog/tpch.properties`` is used to defined the ``tpch`` catalog.  Each connector has their own set
of configuration properites that are specific to the connector.
You can find a connector's configuration properties documented along with the connector.  The TPCH connector has no special
configuration, so we just specify the name of the connector for the catalog, also ``tpch``.

``etc/catalog/tpch.properties``

.. code-block:: none

    connector.name=tpch

We're now ready to build our Docker container specifying the version and then start Presto.
The latest version of Presto is currently |version|.

.. code-block:: none

    docker build --build-arg PRESTO_VERSION=<see releases for latest version> . -t prestodb:latest
    docker run --name presto prestodb:latest

You'll see a series of logs as Presto starts, ending with ``SERVER STARTED`` signaling that it is ready to receive queries.
We'll use the `Presto CLI <https://prestodb.io/docs/current/installation/cli.html>`_ to connect to Presto that we put inside the image
using a separate Terminal window.

.. code-block:: none

    docker exec -it presto presto

We can now execute a query against the `tpch` catalog.

.. code-block:: sql

    presto> SELECT
         ->   l.returnflag,
         ->   l.linestatus,
         ->   sum(l.quantity)                                       AS sum_qty,
         ->   sum(l.extendedprice)                                  AS sum_base_price,
         ->   sum(l.extendedprice * (1 - l.discount))               AS sum_disc_price,
         ->   sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,
         ->   avg(l.quantity)                                       AS avg_qty,
         ->   avg(l.extendedprice)                                  AS avg_price,
         ->   avg(l.discount)                                       AS avg_disc,
         ->   count(*)                                              AS count_order
         -> FROM
         ->   tpch.sf1.lineitem AS l
         -> WHERE
         ->   l.shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
         -> GROUP BY
         ->   l.returnflag,
         ->   l.linestatus
         -> ORDER BY
         ->   l.returnflag,
         ->   l.linestatus;
     returnflag | linestatus |   sum_qty   |    sum_base_price     |    sum_disc_price     |      sum_charge       |      avg_qty       |     avg_price     |       avg_disc       | count_order
    ------------+------------+-------------+-----------------------+-----------------------+-----------------------+--------------------+-------------------+----------------------+-------------
     A          | F          | 3.7734107E7 |  5.658655440072982E10 | 5.3758257134869644E10 |  5.590906522282741E10 | 25.522005853257337 | 38273.12973462155 |  0.04998529583846928 |     1478493
     N          | F          |    991417.0 |  1.4875047103800006E9 |  1.4130821680540998E9 |   1.469649223194377E9 | 25.516471920522985 | 38284.46776084832 |  0.05009342667421586 |       38854
     N          | O          |  7.447604E7 | 1.1170172969773982E11 | 1.0611823030760503E11 | 1.1036704387249734E11 |  25.50222676958499 | 38249.11798890821 |   0.0499965860537345 |     2920374
     R          | F          | 3.7719753E7 |   5.65680413808999E10 |  5.374129268460365E10 |  5.588961911983193E10 |  25.50579361269077 | 38250.85462609959 | 0.050009405830198916 |     1478870
    (4 rows)

    Query 20200625_171123_00000_xqmp4, FINISHED, 1 node
    Splits: 56 total, 56 done (100.00%)
    0:05 [6M rows, 0B] [1.1M rows/s, 0B/s]
