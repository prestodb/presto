=====================
Alluxio Cache Service
=====================

A common optimization to improve Presto query latency is to cache
the working set to avoid unnecessary I/O from remote data sources
or through a slow network. This section describes different options
to leverage Alluxio as a caching layer for Presto.

Using Alluxio File System
-------------------------

Presto can easily use
`Alluxio <https://www.alluxio.io/?utm_source=prestodb&utm_medium=prestodocs>`_
as a distributed caching file system on top of persistent storages, including file systems
like HDFS or object stores like AWS S3, GCP, Azure blob store.
Users may either preload data into Alluxio using Alluxio command-lines before running
Presto queries, or simply rely on Alluxio to transparently cache the most recently or frequently
accessed data based on the data access pattern.

To connect Presto to an Alluxio file system, Hive connector is used to query a Hive metastore
where its tables pointing to ``alluxio://`` address. Refer to the :doc:`/connector/hive` documentation
to learn how to configure Alluxio file system in Presto.

Using Alluxio Structured Data Service
-------------------------------------

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
