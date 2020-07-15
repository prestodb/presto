=====================
Alluxio Cache Service
=====================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

A common optimization to improve Presto query latency is to cache the working set to avoid
unnecessary I/O from remote data sources or through a slow network.
This section describes following options to leverage Alluxio as a caching layer for Presto.

`Alluxio File System <#option1-alluxio-file-system>`_ serves Presto Hive Connector as an independent distributed caching file system on top of HDFS or object stores like AWS S3, GCP, Azure blob store.
Users can understand the cache usage and control cache explicitly through a file system interface.
For example, one can preload all files in an Alluxio directory to warm the cache for Presto queries,
and set the TTL (time-to-live) for cached data to reclaim cache capacity.

`Alluixo Structured Data Service <#option2-alluxio-structured-data-service>`_ interacts with Presto with both a catalog
and a caching file system based on Option1.
This option provides additional benefits on top of option 1 in terms of
seamless access to existing Hive tables without modifying table locations on Hive Metastore and
further performance optimization by consolidating many small files or transforming formats of input files.

Option1: Alluxio File System
----------------------------

Presto Hive connector can connect to
`AlluxioFileSystem <https://docs.alluxio.io/os/user/stable/en/core-services/Caching.html?utm_source=prestodb&utm_medium=prestodocs>`_ as a Hadoop-compatible file system,
on top of other persistent storage systems.

Setup
^^^^^

First, configure ``${PRESTO_HOME}/etc/catalog/hive.properties`` to use the Hive connector.

.. code-block:: none

    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://localhost:9083


Second, ensure the Alluxio client jar is already in
``${PRESTO_HOME}/plugin/hive-hadoop2/`` on all Presto servers.
If this is not the case,
`download Alluxio binary <https://alluxio.io/download>`_, extract the tarball to
``${ALLUXIO_HOME}`` and copy Alluxio client jar
``${ALLUXIO_HOME}/client/alluxio-<VERSION>-client.jar`` into this directory. Restart Presto service:

.. code-block:: none

    $ ${PRESTO_HOME}/bin/launcher restart


Third, configure Hive Metastore connects to Alluxio File System when serving Presto.
Edit ``${HIVE_HOME}/conf/hive-env.sh`` to include Alluxio client jar on the Hive classpath:

.. code-block:: none

    export HIVE_AUX_JARS_PATH=${ALLUXIO_HOME}/client/alluxio-<VERSION>-client.jar

Then restart Hive Metastore

.. code-block:: none

    $ ${HIVE_HOME}/hcatalog/sbin/hcat_server.sh start


Query
^^^^^

After completing the basic configuration, Presto should be able to access Alluxio File System
with tables pointing to ``alluxio://`` address.
Refer to the :doc:`/connector/hive` documentation
to learn how to configure Alluxio file system in Presto. Here is a simple example:

.. code-block:: none

    $ cd ${ALLUXIO_HOME}
    $ bin/alluxio-start.sh local -f
    $ bin/alluxio fs mount --readonly /example \
       s3://apc999/presto-tutorial/example-reason/


Start a Prest CLI connecting to the server started in the previous step.

Download :maven_download:`cli`, rename it to ``presto``,
make it executable with ``chmod +x``, then run it:

.. code-block:: none

    $ ./presto --server localhost:8080 --catalog hive --debug
    presto> use default;
    USE

Create a new table based on the file mounted in Alluxio:

.. code-block:: none

    presto:default> DROP TABLE IF EXISTS reason;
    DROP TABLE
    presto:default> CREATE TABLE reason (
      r_reason_sk integer,
      r_reason_id varchar,
      r_reason_desc varchar
    ) WITH (
      external_location = 'alluxio://localhost:19998/example',
      format = 'PARQUET'
    );
    CREATE TABLE

Scan the newly created table on Alluxio:

.. code-block:: none

    presto:default> SELECT * FROM reason LIMIT 3;
     r_reason_sk |   r_reason_id    |                r_reason_desc
    -------------+------------------+---------------------------------------------
               1 | AAAAAAAABAAAAAAA | Package was damaged
               4 | AAAAAAAAEAAAAAAA | Not the product that was ordred
               5 | AAAAAAAAFAAAAAAA | Parts missing

Basic Operations
^^^^^^^^^^^^^^^^

With Alluxio file system this approach supports the following features:

* **Preloading**: Users can proactively load the working set into Alluxio using command-lines like
  `alluxio fs distributedLoad <https://docs.alluxio.io/os/user/stable/en/operation/User-CLI.html#distributedload>`_,
  in addition to caching data transparently based on the data access pattern.
* **Read/write Types and Data Policies**: Users can customize read and write modes for Presto when reading from and writing to Alluxio.
  E.g.  tell Presto read to skip caching data when reading from certain locations and avoid cache thrashing, or set TTLs on files in given locations using
  `alluxio fs setTtl <https://docs.alluxio.io/os/user/stable/en/operation/User-CLI.html#setttl>`_.
* **Check Working Set**: Users can verify which files are cached to understand and optimize Presto performance. For example, users can check the output from Alluxio command line
  `alluxio fs ls <https://docs.alluxio.io/os/user/stable/en/operation/User-CLI.html#ls>`_,
  or browse the corresponding files on
  `Alluxio WebUI <https://docs.alluxio.io/os/user/stable/en/operation/Web-Interface.html>`_.
* **Check Resource Utilization**: System admins can monitor how much of the cache capacity on each node is used using
  `alluxio fsadmin report <https://docs.alluxio.io/os/user/stable/en/operation/Admin-CLI.html#report>`_ and plan the resource accordingly.

Option2: Alluxio Structured Data Service
----------------------------------------

In addition to caching data as a file system, Alluxio can further provide data
abstracted as tables and via the Alluxio Structured Data Service. The `Alluxio
catalog <https://docs.alluxio.io/os/user/stable/en/core-services/Catalog.html?utm_source=prestodb&utm_medium=prestodocs>`_
is the main component responsible for managing the structured data metadata, and
caching that information from the underlying table metastore (such as Hive
Metastore). After an existing table metastore is
`attached <https://docs.alluxio.io/os/user/stable/en/operation/User-CLI.html#attachdb>`_
to the Alluxio catalog, the catalog will cache the table metadata from the
underlying metastore, and serve that information to Presto. When Presto accesses
the Alluxio catalog for table metadata, the Alluxio catalog will automatically
use the Alluxio locations of the files, which removes the need to modify any
locations in the existing Hive Metastore. Therefore, when Presto is using the
Alluxio catalog, the table metadata is cached in the catalog, and the file
contents are cached with Alluxio’s file system caching.

For example, a user can attach an existing Hive Metastore to the Alluxio catalog:

.. code-block:: none

    ./bin/alluxio table attachdb hive thrift://METASTORE_HOSTNAME:9083 hive_db_name

Then configure a Presto catalog to connect to the Alluxio catalog:

.. code-block:: none

    connector.name=hive-hadoop2
    hive.metastore=alluxio
    hive.metastore.alluxio.master.address=ALLUXIO_HOSTNAME:19998

Now, Presto queries can utilize both the file caching and structured data caching
provided by Alluxio. Please read :doc:`/connector/hive` for more details.
