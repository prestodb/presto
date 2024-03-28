==========
Connectors
==========

Connectors allow reading and writing data to and from external sources.
This concept is similar to `Presto Connectors <https://prestodb.io/docs/current/develop/connectors.html>`_.
The :ref:`TableScanNode<TableScanNode>` operator reads external data via a connector.
The :ref:`TableWriteNode<TableWriteNode>` operator writes data externally via a connector.
The various connector interfaces in Velox are described below.

Connector Interface
-------------------

.. list-table::
   :widths: 10 40
   :header-rows: 1

   * - Interface Name
     - Description
   * - ConnectorSplit
     - A chunk of data to process. For example, a single file.
   * - DataSource
     - Provides methods to consume and process a split. A DataSource can optionally consume a
       dynamic filter during execution to prune some rows from the output vector.
   * - DataSink
     - Provides methods to write a Velox vector externally.
   * - Connector
     - Allows creating instances of a DataSource or a DataSink.
   * - Connector Factory
     - Enables creating instances of a particular connector.

Velox provides Hive and TPC-H Connectors out of the box.
Let's see how the above connector interfaces are implemented in the Hive Connector in detail below.

Hive Connector
--------------
The Hive Connector is used to read and write data files (Parquet, DWRF) residing on
an external storage (S3, HDFS, GCS, Linux FS).

HiveConnectorSplit
~~~~~~~~~~~~~~~~~~
The HiveConnectorSplit describes a data chunk using parameters including `file-path`,
`file-format`, `start`, `length`, `storage format`, etc..
It is not necessary to specify start and length values that align with row boundaries.
For example, in a Parquet file, those row groups with offset in the range of [start, length)
are processed as part of the split.
For a given a set of files, users or applications are responsible for defining the splits.

HiveDataSource
~~~~~~~~~~~~~~
The HiveDataSource implements the `addSplit` API that consumes a HiveConnectorSplit.
It creates a file reader based on the file format, offset, and length. The supported file formats
are DWRF and Parquet.
The `next` API processes the split and returns a batch of rows. Users can continue to call
`next` until all the rows in the split are fully read.
HiveDataSource allows adding a dynamic filter using the `addDynamicFilter` API. This allows
supporting :ref:`Dynamic Filter Pushdown<DynamicFilterPushdown>`.

HiveDataSink
~~~~~~~~~~~~
The HiveDataSink writes vectors to files on disk. The supported file formats are DWRF and Parquet.
The parameters to HiveDataSink also include column names, sorting, partitioning, and bucketing information.
The `appendData` API instantiates a file writer based on the above parameters and writes a vector to disk.

HiveConnector
~~~~~~~~~~~~~
The HiveConnector implements the `createDataSource` connector API to create instances of HiveDataSource.
It also implements the `createDataSink` connector API to create instances of HiveDataSink.
One of the parameters to these APIs is `ConnectorQueryCtx`, which provides means to specify a
memory pool and connector configuration.

HiveConnectorFactory
~~~~~~~~~~~~~~~~~~~~
The HiveConnectorFactory enabled creating instances of the HiveConnector. A `connector name` say "hive"
is required to register the HiveConnectorFactory. Multiple instances of the HiveConnector can then be
created by using the `newConnector` API by specifying a `connectorId` and connector configuration listed
:doc:`here</configs>`. Multiple instances of a connector are required if you have multiple external
sources and each require a different configuration.

Storage Adapters
~~~~~~~~~~~~~~~~
Hive Connector allows reading and writing files from a variety of distributed storage systems.
The supported storage API are S3, HDFS, GCS, Linux FS.

If file is not found when reading, `openFileForRead` API throws `VeloxRuntimeError` with `error_code::kFileNotFound`.
This behavior is necessary to support the `ignore_missing_files` configuration property.

S3 is supported using the `AWS SDK for C++ <https://github.com/aws/aws-sdk-cpp>`_ library.
S3 supported schemes are `s3://` (Amazon S3, Minio), `s3a://` (Hadoop 3.x), `s3n://` (Deprecated in Hadoop 3.x),
`oss://` (Alibaba cloud storage), and `cos://`, `cosn://` (Tencent cloud storage).

HDFS is supported using the
`Apache Hawk libhdfs3 <https://github.com/apache/hawq/tree/master/depends/libhdfs3>`_ library. HDFS supported schemes
are `hdfs://`.

GCS is supported using the
`Google Cloud Platform C++ Client Libraries <https://github.com/googleapis/google-cloud-cpp>`_. GCS supported schemes
are `gs://`.

ABS (Azure Blob Storage) is supported using the
`Azure SDK for C++ <https://github.com/Azure/azure-sdk-for-cpp>`_ library. ABS supported schemes are `abfs(s)://`.

S3 Storage adapter using a proxy
********************************

By default, the C++ AWS S3 client does not honor the configuration of the
environment variables http_proxy, https_proxy, and no_proxy.
The Java AWS S3 client supports this.
The environment variables can be specified as lower case, upper case or both.
In order to enable the use of a proxy the hive connector configuration variable
`hive.s3.use-proxy-from-env` must be set to `true`. By default, the value
is `false`.

This is the behavior when the proxy settings are enabled:

1. http_proxy/HTTP_PROXY, https_proxy/HTTPS_PROXY and no_proxy/NO_PROXY
   environment variables are read. If lower case and upper case variables are set
   lower case variables take precendence.
2. The no_proxy/NO_PROXY content is scanned for exact and suffix matches.
3. IP addresses, domains, subdomains, or IP ranges (CIDR) can be specified in no_proxy/NO_PROXY.
4. The no_proxy/NO_PROXY list is comma separated.
5. Use . or \*. to indicate domain suffix matching, e.g. `.foobar.com` will
   match `test.foobar.com` or `foo.foobar.com`.
