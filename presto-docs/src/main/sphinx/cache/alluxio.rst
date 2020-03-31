=====================
Alluxio Cache Service
=====================

A common optimization to improve Presto query latency is to cache
the working set to avoid unnecessary I/O from remote data sources
or through slow network.

Caching Data in Alluxio Service
-------------------------------

Presto can easily use
`Alluxio <https://www.alluxio.io/?utm_source=prestodb&utm_medium=prestodocs>`_
as a distributed caching layer on top of persistent storages, including file systems
like HDFS or object stores like AWS S3, GCP, Azure blob store.
Users may either preload data into Alluxio using Alluxio command-lines before running
Presto queries, or simply rely on Alluxio to transparently cache the most recently or frequently
accessed data based on the data access pattern.

To connect Presto to an Alluxio service, Hive connector is used to query a Hive metastore
where its tables pointing to ``alluxio://`` address. Refer to the :doc:`/connector/hive` documentation
to learn how to configure Alluxio in Presto.
