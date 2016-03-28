==============
Hive Connector
==============

The Hive connector allows querying data stored in a Hive
data warehouse. Hive is a combination of three components:

* Data files in varying formats that are typically stored in the
  Hadoop Distributed File System (HDFS) or in Amazon S3.
* Metadata about how the data files are mapped to schemas and tables.
  This metadata is stored in a database such as MySQL and is accessed
  via the Hive metastore service.
* A query language called HiveQL. This query language is executed
  on a distributed computing framework such as MapReduce or Tez.

Presto only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

Configuration
-------------

Presto includes Hive connectors for multiple versions of Hadoop:

* ``hive-hadoop1``: Apache Hadoop 1.x
* ``hive-hadoop2``: Apache Hadoop 2.x
* ``hive-cdh4``: Cloudera CDH 4
* ``hive-cdh5``: Cloudera CDH 5

Create ``etc/catalog/hive.properties`` with the following contents
to mount the ``hive-cdh4`` connector as the ``hive`` catalog,
replacing ``hive-cdh4`` with the proper connector for your version
of Hadoop and ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: none

    connector.name=hive-cdh4
    hive.metastore.uri=thrift://example.net:9083

Multiple Hive Clusters
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

HDFS Configuration
^^^^^^^^^^^^^^^^^^

For basic setups, Presto configures the HDFS client automatically and
does not require any configuration files. In some cases, such as when using
federated HDFS or NameNode high availability, it is necessary to specify
additional HDFS client options in order to access your HDFS cluster. To do so,
add the ``hive.config.resources`` property to reference your HDFS config files:

.. code-block:: none

    hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml

Only specify additional configuration files if necessary for your setup.
We also recommend reducing the configuration files to have the minimum
set of required properties, as additional properties may cause problems.

The configuration files must exist on all Presto nodes. If you are
referencing existing Hadoop config files, make sure to copy them to
any Presto nodes that are not running Hadoop.

Configuration Properties
------------------------

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.metastore.uri``                             The URI(s) of the Hive metastore to connect to using the
                                                   Thrift protocol. If multiple URIs are provided, the first
                                                   URI is used by default and the rest of the URIs are
                                                   fallback metastores. This property is required.
                                                   Example: ``thrift://192.0.2.3:9083`` or
                                                   ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.config.resources``                          An optional comma-separated list of HDFS
                                                   configuration files. These files must exist on the
                                                   machines running Presto. Only specify this if
                                                   absolutely necessary to access HDFS.
                                                   Example: ``/etc/hdfs-site.xml``

``hive.storage-format``                            The default file format used when creating new tables.       ``RCBINARY``

``hive.compression-codec``                         The compression codec to use when writing files.             ``GZIP``

``hive.force-local-scheduling``                    See in :ref:`tuning section<force-local-scheduling>`         ``false``

``hive.allow-drop-table``                          Allow the Hive connector to drop tables.                     ``false``

``hive.allow-rename-table``                        Allow the Hive connector to rename tables.                   ``false``

``hive.respect-table-format``                      Should new partitions be written using the existing table    ``true``
                                                   format or the default Presto format?

``hive.immutable-partitions``                      Can new data be inserted into existing partitions?           ``false``

``hive.max-partitions-per-writers``                Maximum number of partitions per writer.                     100
================================================== ============================================================ ==========

Amazon S3 Configuration
^^^^^^^^^^^^^^^^^^^^^^^

The Hive connector also allows querying data stored in Amazon S3.

To access tables stored in S3, you must specify the AWS credential properties 
``hive.s3.aws-access-key`` and ``hive.s3.aws-secret-key``. Alternatively, you can use
``hive.s3.use-instance-credentials`` which if set to true, enables retrieving temporary
`instance profile <http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-roles.html>`_
AWS credentials.

SQL Limitation for S3 tables
----------------------------

The SQL support for S3 tables is the same as for HDFS tables. Presto does not support creating external
tables in Hive (both HDFS and S3). If you want to create a table in Hive with data in S3, you have to do it from
`Hive <https://cwiki.apache.org/confluence/display/Hive/HiveAws+HivingS3nRemotely>`_.

Also, ``CREATE TABLE..AS query``, where ``query`` is a ``SELECT`` query on the S3 table will not create the table
on S3. If you want to load data back to S3, you need to use ``INSERT INTO`` command.

Configuration Properties
------------------------

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.s3.aws-access-key``                         AWS Access key.

``hive.s3.aws-secret-key``                         AWS Secret key.

``hive.s3.use-instance-credentials``               Instance profile credentials to use. This property is unused ``true``
                                                   if default credential properties are added.
	 	 	 
``hive.s3.connect-timeout``                        Amount of time that the HTTP connection will wait to         ``5s``
                                                   establish a connection before giving up.

``hive.s3.socket-timeout``                         Amount of time to wait for data to be transferred over an    ``5s``
                                                   established, open connection before the connection times 
                                                   out and is closed.

``hive.s3.max-error-retries``                      Maximum retry count for retriable errors.                    ``10``

``hive.s3.max-connections``                        See :ref:`tuning section<s3-max-connections>`.               ``500``

``hive.s3.ssl.enabled``                            Protocol to connect to AWS (HTTP or HTTPS).                  ``true``

``hive.s3.pin-client-to-current-region``           Use current AWS region.                                      ``false``

``hive.s3.max-backoff-time``                       Maximum value of sleep time allowed during data read retry   ``10 minutes``
                                                   mechanism. Uses exponential backoff pattern ranging from
                                                   1s to this value.

``hive.s3.max-retry-time``                         Retries read attempt till this threshold is reached or       ``10 minutes``
                                                   ``hive.s3.max-client-retries`` value is crossed.

``hive.s3.max-client-retries``                     Reader fails if either ``hive.s3.max-retry-time``            ``3``
                                                   is reached or the number of attempts hits this value.

``hive.s3.multipart.min-file-size``                See :ref:`tuning section<s3-multipart-min-file>`.            ``16MB``
                                                
``hive.s3.multipart.min-part-size``                See :ref:`tuning section<s3-multipart-min-part>`.            ``5MB``

``hive.s3.sse.enabled``                            Enable S3 server side encryption.                            ``false``

``hive.s3.staging-directory``                      Temporary directory for staging files before uploading       ``/tmp``
                                                   to S3.
================================================== ============================================================ ==========

Querying Hive Tables
--------------------

The following table is an example Hive table from the `Hive Tutorial`_.
It can be created in Hive (not in Presto) using the following
Hive ``CREATE TABLE`` command:

.. _Hive Tutorial: https://cwiki.apache.org/confluence/display/Hive/Tutorial#Tutorial-UsageandExamples

.. code-block:: none

    hive> CREATE TABLE page_view (
        >   viewTime INT,
        >   userid BIGINT,
        >   page_url STRING,
        >   referrer_url STRING,
        >   ip STRING COMMENT 'IP Address of the User')
        > COMMENT 'This is the page view table'
        > PARTITIONED BY (dt STRING, country STRING)
        > STORED AS SEQUENCEFILE;
    OK
    Time taken: 3.644 seconds

Assuming that this table was created in the ``web`` schema in
Hive, this table can be described in Presto::

    DESCRIBE hive.web.page_view;

.. code-block:: none

        Column    |  Type   | Null | Partition Key |        Comment
    --------------+---------+------+---------------+------------------------
     viewtime     | bigint  | true | false         |
     userid       | bigint  | true | false         |
     page_url     | varchar | true | false         |
     referrer_url | varchar | true | false         |
     ip           | varchar | true | false         | IP Address of the User
     dt           | varchar | true | true          |
     country      | varchar | true | true          |
    (7 rows)

This table can then be queried in Presto::

    SELECT * FROM hive.web.page_view;


.. _tuning-pref-hive:

Tuning
-------

The following configuration properties may have an impact on connector performance:

``hive.assume-canonical-partition-keys``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Disable optimized metastore partition fetching for non-string partition keys. Setting this property allows to avoid ignoring data with non-canonical partition values.


``hive.domain-compaction-threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``100``
 * **Description:** Maximum number of ranges allowed in a tuple domain without compacting it. Higher value will cause more data fragmentation but allows to use row skipping feature when reading ORC data. Setting this value higher may have large impact on ``IN`` and ``OR`` clauses performance in scenarios making use of row skipping.


.. _force-local-scheduling:

``hive.force-local-scheduling``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Force splits to be scheduled on the same node (ignoring normal node selection procedures) as the Hadoop DataNode process serving the split data. This is useful for installations where Presto is collocated with every DataNode and may increase queries time significantly. The drawback may be that if some data are accessed more often, the utilization of some nodes may be low even if the whole system is heavy loaded. See also :ref:`node-scheduler.network-topology<node-scheduler-network-topology>` if less strict constrain is preferred - especially if some nodes are overloaded and other are not fully utilized.


``hive.max-initial-split-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``hive.max-split-size`` / ``2`` (``32 MB``)
 * **Description:** This property describes max size of each of initially created splits for a single query. The logic of initial splits is described in ``hive.max-initial-splits`` property. Changing this value changes what is considered small query. Higher value causes smaller parallelism for small queries. Lower value increases concurrency for them. This is max size, as the real size may be lower when end of blocks in single DataNode is reached.


``hive.max-initial-splits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer``
 * **Default value:** ``200``
 * **Description:** This property describes how many splits may be initially created for a single query. The initial splits are created to allow better concurrency for small queries. Hive connector will create first ``hive.max-initial-splits`` splits with size of ``hive.max-initial-split-size`` instead of ``hive.max-split-size``. Having this value higher will force more splits to have smaller size effectively increasing definition of what is considered small query in database.


``hive.max-outstanding-splits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** Limit of number of splits waiting to be served by split source. After reaching this limit writers will stop writing new splits to split source until some of them are used by workers. Higher value will increase memory usage, but will allow to concentrate all IO at one time which may be much faster and increase resources utilization.


``hive.max-partitions-per-writers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``100``
 * **Description:** Maximum number of partitions per writer. If higher number of partitions per writer will be required to complete query, the query will fail. By manipulating this value one may change how large queries are meant to be dropped from DB which may help with error detection.


``hive.max-split-iterator-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``1000``
 * **Description:** This property describes how many threads may be used to iterate through splits when loading them to the worker nodes. Higher value may increase parallelism, but high concurrency may cause time being wasted on context switching.


``hive.max-split-size``
^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``64 MB``
 * **Description:** This value describes max size of split that is created after using all ``hive-max-initial-split-size`` of initial splits. The logic of initial splits is described in ``hive.max-initial-splits``. Having this value higher causes smaller parallelism which may be desirable when queries are very large and cluster is stable allowing to process data locally more efficiently without wasting time for context switching, synchronization and data collecting. The optimal value should be aligned with average query size in system.


``hive.metastore.partition-batch-size.max``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``100``
 * **Description:** This together with ``hive.metastore.partition-batch-size.min`` defines range of partition sizes read from Hive. First partition is always of size ``hive.metastore.partition-batch-size.min`` and each following partition is two times bigger then previous up to ``hive.mestastore.partition-batch-size.max`` (the formula for ``n`` partition size is min(``hive.metastore.partition-batch-size.max``, (``2``^``n``) * ``hive.metastore.partition-batch-size.min``)). This algorithm allows to adjust partition size live to what is required. If size of queries in system differs siginificantly, then this range should be extended to better adjust to processed case. In case of cluster working with queries with about the same size, both values may be same for maximal attunement giving slight edge in processing time.


``hive.metastore.partition-batch-size.min``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``10``
 * **Description:** See ``hive.metastore.partition-batch-size.max``.


``hive.optimized-reader.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** *Deprecated* Enables number of reader improvements introduced by alternative ORC implementation. The new reader supports vectorized reads, lazy loading, and predicate push down, all of which make the reader more efficient and typically reduces wall clock time for a query. However as the code has changed significantly it may or may not introduce some minor issues, so it can be disabled if some  problems with environment are noticed.


``hive.orc.max-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``8 MB``
 * **Description:** Serves as default value for ``orc_max_buffer_size`` and ``orc_stream_buffer_size`` session properties defining max size of ORC read or streaming operators. Higher value will allow bigger chunks to be processed but will decrease concurrency level.


``hive.orc.max-merge-distance``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``1 MB``
 * **Description:** Serves as default value for ``orc_max_merge_distance`` session property. Defines maximum size of gap between two reads to merge into a single read. The reads may be merged if distance between requested data ranges in data source is smaller or equal to this value.


``hive.orc.stream-buffer-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size)
 * **Default value:** ``8 MB``
 * **Description:** *Unused*

.. _parquet-optimized-reader:

``hive.parquet-optimized-reader.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** *Deprecated* Serves as default value for ``parquet_optimized_reader_enabled`` session property. Enables number of reader improvements introduced by alternative parquet implementation. The new reader supports vectorized reads, lazy loading, and predicate push down, all of which make the reader more efficient and typically reduces wall clock time for a query. However as the code has changed significantly it may or may not introduce some minor issues, so it can be disabled if some  problems with environment are noticed. This property enables/disables all optimizations except of predicate pushdown as it is managed by ``hive.parquet-predicate-pushdown.enabled`` property.


``hive.parquet-predicate-pushdown.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** *Deprecated* Serves as default value for ``parquet_predicate_pushdown_enabled`` sesssion property. See :ref:`hive.parquet-optimized-reader.enabled<parquet-optimized-reader>`.


``hive.parquet.use-column-names``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Boolean``
 * **Default value:** ``false``
 * **Description:** Access Parquet columns using names from the file. By default, columns in Parquet files are accessed by their ordinal position in the Hive table definition. Setting this property allows to use columns names recorded in the Parquet file instead.

.. _s3-max-connections:

``hive.s3.max-connections``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``Integer`` (at least ``1``)
 * **Default value:** ``500``
 * **Description:** This value the maximum number of connections to S3. How many connection to S3 cluster may be open at the same time by the S3 driver. Higher value may increase network utilization when cluster is used on high speed network. However higher value relies more on S3 servers being well configured for high parallelism.

.. _s3-multipart-min-file:

``hive.s3.multipart.min-file-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size, at least ``16 MB``)
 * **Default value:** ``16 MB``
 * **Description:** Minimum file size for an S3 multipart upload. This property describes how big file must be to be uploaded to S3 cluster using multipart feature. Amazon recommendation is to use ``100 MB`` value here, however lower value may allow to increase upload parallelism and can decrease ``data lost``/``data sent`` ratio in unstable network conditions.

.. _s3-multipart-min-part:

``hive.s3.multipart.min-part-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 * **Type:** ``String`` (data size, at least ``5 MB``)
 * **Default value:** ``5 MB``
 * **Description:** Defines the minimum part size for upload parts. Decreasing the minimum part size causes multipart uploads to be split into a larger number of smaller parts. Setting this value too low has a negative effect on transfer speeds, causing extra latency and network communication for each part.

Character data types
--------------------

Hive supports three character data types:
 - ``STRING``
 - ``CHAR(n)``
 - ``VARCHAR(n)``

Currently columns for all those data types are exposed in presto as unparametrized ``VARCHAR`` type.
This implies semantic inconsistencies for columns defined as ``CHAR(x)`` between Hive and Presto.

Following example documents basic semantic differences:

**Create table in Hive**

.. code-block:: none

    hive> create table string_test (c char(5), v varchar(5), s string) stored as orc;
    hive> insert into string_test values ('ala', 'ala', 'ala'), ('ala ', 'ala ', 'ala ');


**Query the table in Hive**

.. code-block:: none

    hive> select concat('x', c, 'x'), concat('x', v, 'x'), concat('x', s, 'x'), length(c), length(v), length(s) from string_test;
    OK
    xalax	xalax	 xalax	 3	3	3
    xalax	xala x	 xala x	 3	4	4

**Query the table in Presto**

.. code-block:: none

    presto:default> select concat('x',c,'x'), concat('x', v, 'x'), concat('x', s, 'x'), length(c), length(v), length(s) from string_test;
      _col0  | _col1  | _col2  | _col3 | _col4 | _col5
    ---------+--------+--------+-------+-------+-------
     xala  x | xalax  | xalax  |     5 |     3 |     3
     xala  x | xala x | xala x |     5 |     4 |     4

Also for ``CHAR(x)`` datatype padding whitespace should not be taken into consideration during comparisons.
So ``'ala  '`` should be equal to ``'ala        '``. This is currently not the case in Presto.


**Note:** Ultimately Presto presto will implement native ``CHAR(x)`` data type. It will follow ANSI SQL semantics which differs from
 Hive's. This will cause backward incompatibilities of queries using Hive's ``CHAR(x)`` columns.
