==============
Hive Connector
==============

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

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

Supported File Types
--------------------

The following file types are supported for the Hive connector:

* ORC
* Parquet
* Avro
* RCFile
* SequenceFile
* JSON
* Text

Configuration
-------------

The Hive connector supports Apache Hadoop 2.x and derivative distributions
including Cloudera CDH 5 and Hortonworks Data Platform (HDP).

Create ``etc/catalog/hive.properties`` with the following contents
to mount the ``hive-hadoop2`` connector as the ``hive`` catalog,
replacing ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: none

    connector.name=hive-hadoop2
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

HDFS Username
^^^^^^^^^^^^^

When not using Kerberos with HDFS, Presto will access HDFS using the
OS user of the Presto process. For example, if Presto is running as
``nobody``, it will access HDFS as ``nobody``. You can override this
username by setting the ``HADOOP_USER_NAME`` system property in the
Presto :ref:`presto_jvm_config`, replacing ``hdfs_user`` with the
appropriate username:

.. code-block:: none

    -DHADOOP_USER_NAME=hdfs_user

Accessing Hadoop clusters protected with Kerberos authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Kerberos authentication is supported for both HDFS and the Hive metastore.
However, Kerberos authentication by ticket cache is not yet supported.

The properties that apply to Hive connector security are listed in the
`Hive Configuration Properties`_ table. Please see the
:doc:`/connector/hive-security` section for a more detailed discussion of the
security options in the Hive connector.

Hive Configuration Properties
-----------------------------

================================================== ============================================================ ============
Property Name                                      Description                                                  Default
================================================== ============================================================ ============
``hive.metastore.uri``                             The URI(s) of the Hive metastore to connect to using the
                                                   Thrift protocol. If multiple URIs are provided, the first
                                                   URI is used by default and the rest of the URIs are
                                                   fallback metastores. This property is required.
                                                   Example: ``thrift://192.0.2.3:9083`` or
                                                   ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.metastore.username``                        The username Presto will use to access the Hive metastore.

``hive.config.resources``                          An optional comma-separated list of HDFS
                                                   configuration files. These files must exist on the
                                                   machines running Presto. Only specify this if
                                                   absolutely necessary to access HDFS.
                                                   Example: ``/etc/hdfs-site.xml``

``hive.storage-format``                            The default file format used when creating new tables.       ``ORC``

``hive.compression-codec``                         The compression codec to use when writing files.             ``GZIP``

``hive.force-local-scheduling``                    Force splits to be scheduled on the same node as the Hadoop  ``false``
                                                   DataNode process serving the split data.  This is useful for
                                                   installations where Presto is collocated with every
                                                   DataNode.

``hive.respect-table-format``                      Should new partitions be written using the existing table    ``true``
                                                   format or the default Presto format?

``hive.immutable-partitions``                      Can new data be inserted into existing partitions?           ``false``

``hive.create-empty-bucket-files``                 Should empty files be created for buckets that have no data? ``true``

``hive.max-partitions-per-writers``                Maximum number of partitions per writer.                     100

``hive.max-partitions-per-scan``                   Maximum number of partitions for a single table scan.        100,000

``hive.metastore.authentication.type``             Hive metastore authentication type.                          ``NONE``
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.metastore.service.principal``               The Kerberos principal of the Hive metastore service.

``hive.metastore.client.principal``                The Kerberos principal that Presto will use when connecting
                                                   to the Hive metastore service.

``hive.metastore.client.keytab``                   Hive metastore client keytab location.

``hive.hdfs.authentication.type``                  HDFS authentication type.                                    ``NONE``
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.hdfs.impersonation.enabled``                Enable HDFS end user impersonation.                          ``false``

``hive.hdfs.presto.principal``                     The Kerberos principal that Presto will use when connecting
                                                   to HDFS.

``hive.hdfs.presto.keytab``                        HDFS client keytab location.

``hive.security``                                  See :doc:`hive-security`.

``security.config-file``                           Path of config file to use when ``hive.security=file``.
                                                   See :ref:`hive-file-based-authorization` for details.

``hive.non-managed-table-writes-enabled``          Enable writes to non-managed (external) Hive tables.         ``false``

``hive.non-managed-table-creates-enabled``         Enable creating non-managed (external) Hive tables.          ``true``

``hive.collect-column-statistics-on-write``        Enables automatic column level statistics collection         ``false``
                                                   on write. See `Table Statistics <#table-statistics>`__ for
                                                   details.

``hive.s3select-pushdown.enabled``                 Enable query pushdown to AWS S3 Select service.              ``false``

``hive.s3select-pushdown.max-connections``         Maximum number of simultaneously open connections to S3 for  500
                                                   S3SelectPushdown.
``hive.metastore.load-balancing-enabled``       Enable load balancing between multiple Metastore instances
================================================== ============================================================ ============

Metastore Configuration Properties
----------------------------------

The required Hive metastore can be configured with a number of properties.

======================================= ============================================================ ============
Property Name                                      Description                                       Default
======================================= ============================================================ ============
``hive.metastore-timeout``              Timeout for Hive metastore requests.                         ``10s``

``hive.metastore-cache-ttl``            Duration how long cached metastore data should be considered ``0s``
                                        valid.

``hive.metastore-cache-maximum-size``   Hive metastore cache maximum size.                            10000

``hive.metastore-refresh-interval``     Asynchronously refresh cached metastore data after access    ``0s``
                                        if it is older than this but is not yet expired, allowing
                                        subsequent accesses to see fresh data.

``hive.metastore-refresh-max-threads``  Maximum threads used to refresh cached metastore data.        100

======================================= ============================================================ ============

AWS Glue Catalog Configuration Properties
-----------------------------------------

==================================================== ============================================================
Property Name                                        Description
==================================================== ============================================================
``hive.metastore.glue.region``                       AWS region of the Glue Catalog. This is required when not
                                                     running in EC2, or when the catalog is in a different region.
                                                     Example: ``us-east-1``

``hive.metastore.glue.pin-client-to-current-region`` Pin Glue requests to the same region as the EC2 instance
                                                     where Presto is running (defaults to ``false``).

``hive.metastore.glue.max-connections``              Max number of concurrent connections to Glue
                                                     (defaults to ``5``).

``hive.metastore.glue.max-error-retries``            Maximum number of error retries for the Glue client,
                                                     defaults to ``10``.

``hive.metastore.glue.default-warehouse-dir``        Hive Glue metastore default warehouse directory

``hive.metastore.glue.aws-access-key``               AWS access key to use to connect to the Glue Catalog. If
                                                     specified along with ``hive.metastore.glue.aws-secret-key``,
                                                     this parameter takes precedence over
                                                     ``hive.metastore.glue.iam-role``.

``hive.metastore.glue.aws-secret-key``               AWS secret key to use to connect to the Glue Catalog. If
                                                     specified along with ``hive.metastore.glue.aws-access-key``,
                                                     this parameter takes precedence over
                                                     ``hive.metastore.glue.iam-role``.

``hive.metastore.glue.catalogid``                    The ID of the Glue Catalog in which the metadata database
                                                     resides.

``hive.metastore.glue.endpoint-url``                 Glue API endpoint URL (optional).
                                                     Example: ``https://glue.us-east-1.amazonaws.com``

``hive.metastore.glue.partitions-segments``          Number of segments for partitioned Glue tables.

``hive.metastore.glue.get-partition-threads``        Number of threads for parallel partition fetches from Glue.

``hive.metastore.glue.iam-role``                     ARN of an IAM role to assume when connecting to the Glue
                                                     Catalog.
==================================================== ============================================================

.. _s3selectpushdown:

Amazon S3 Configuration
-----------------------

The Hive Connector can read and write tables that are stored in S3.
This is accomplished by having a table or database location that
uses an S3 prefix rather than an HDFS prefix.

Presto uses its own S3 filesystem for the URI prefixes
``s3://``, ``s3n://`` and  ``s3a://``.

S3 Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^

============================================ =================================================================
Property Name                                Description
============================================ =================================================================
``hive.s3.use-instance-credentials``         Use the EC2 metadata service to retrieve API credentials
                                             (defaults to ``true``). This works with IAM roles in EC2.

``hive.s3.aws-access-key``                   Default AWS access key to use.

``hive.s3.aws-secret-key``                   Default AWS secret key to use.

``hive.s3.iam-role``                         IAM role to assume.

``hive.s3.endpoint``                         The S3 storage endpoint server. This can be used to
                                             connect to an S3-compatible storage system instead
                                             of AWS. When using v4 signatures, it is recommended to
                                             set this to the AWS region-specific endpoint
                                             (e.g., ``http[s]://<bucket>.s3-<AWS-region>.amazonaws.com``).

``hive.s3.storage-class``                    The S3 storage class to use when writing the data. Currently only
                                             ``STANDARD`` and ``INTELLIGENT_TIERING`` storage classes are supported.
                                             Default storage class is ``STANDARD``

``hive.s3.signer-type``                      Specify a different signer type for S3-compatible storage.
                                             Example: ``S3SignerType`` for v2 signer type

``hive.s3.path-style-access``                Use path-style access for all requests to the S3-compatible storage.
                                             This is for S3-compatible storage that doesn't support virtual-hosted-style access.
                                             (defaults to ``false``)

``hive.s3.staging-directory``                Local staging directory for data written to S3.
                                             This defaults to the Java temporary directory specified
                                             by the JVM system property ``java.io.tmpdir``.

``hive.s3.pin-client-to-current-region``     Pin S3 requests to the same region as the EC2
                                             instance where Presto is running (defaults to ``false``).

``hive.s3.ssl.enabled``                      Use HTTPS to communicate with the S3 API (defaults to ``true``).

``hive.s3.sse.enabled``                      Use S3 server-side encryption (defaults to ``false``).

``hive.s3.sse.type``                         The type of key management for S3 server-side encryption.
                                             Use ``S3`` for S3 managed or ``KMS`` for KMS-managed keys
                                             (defaults to ``S3``).

``hive.s3.sse.kms-key-id``                   The KMS Key ID to use for S3 server-side encryption with
                                             KMS-managed keys. If not set, the default key is used.

``hive.s3.kms-key-id``                       If set, use S3 client-side encryption and use the AWS
                                             KMS to store encryption keys and use the value of
                                             this property as the KMS Key ID for newly created
                                             objects.

``hive.s3.encryption-materials-provider``    If set, use S3 client-side encryption and use the
                                             value of this property as the fully qualified name of
                                             a Java class which implements the AWS SDK's
                                             ``EncryptionMaterialsProvider`` interface.   If the
                                             class also implements ``Configurable`` from the Hadoop
                                             API, the Hadoop configuration will be passed in after
                                             the object has been created.

``hive.s3.upload-acl-type``                  Canned ACL to use while uploading files to S3 (defaults
                                             to ``Private``).
``hive.s3.skip-glacier-objects``             Ignore Glacier objects rather than failing the query. This
                                             will skip data that may be expected to be part of the table
                                             or partition. Defaults to ``false``.
============================================ =================================================================

S3 Credentials
^^^^^^^^^^^^^^

If you are running Presto on Amazon EC2 using EMR or another facility,
it is highly recommended that you set ``hive.s3.use-instance-credentials``
to ``true`` and use IAM Roles for EC2 to govern access to S3. If this is
the case, your EC2 instances will need to be assigned an IAM Role which
grants appropriate access to the data stored in the S3 bucket(s) you wish
to use. It's also possible to configure an IAM role with ``hive.s3.iam-role``
that will be assumed for accessing any S3 bucket. This is much cleaner than
setting AWS access and secret keys in the ``hive.s3.aws-access-key``
and ``hive.s3.aws-secret-key`` settings, and also allows EC2 to automatically
rotate credentials on a regular basis without any additional work on your part.

Custom S3 Credentials Provider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can configure a custom S3 credentials provider by setting the Hadoop
configuration property ``presto.s3.credentials-provider`` to be the
fully qualified class name of a custom AWS credentials provider
implementation. This class must implement the
`AWSCredentialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html>`_
interface and provide a two-argument constructor that takes a
``java.net.URI`` and a Hadoop ``org.apache.hadoop.conf.Configuration``
as arguments. A custom credentials provider can be used to provide
temporary credentials from STS (using ``STSSessionCredentialsProvider``),
IAM role-based credentials (using ``STSAssumeRoleSessionCredentialsProvider``),
or credentials for a specific use case (e.g., bucket/user specific credentials).
This Hadoop configuration property must be set in the Hadoop configuration
files referenced by the ``hive.config.resources`` Hive connector property.

Tuning Properties
^^^^^^^^^^^^^^^^^

The following tuning properties affect the behavior of the client
used by the Presto S3 filesystem when communicating with S3.
Most of these parameters affect settings on the ``ClientConfiguration``
object associated with the ``AmazonS3Client``.

===================================== =========================================================== ===============
Property Name                         Description                                                 Default
===================================== =========================================================== ===============
``hive.s3.max-error-retries``         Maximum number of error retries, set on the S3 client.      ``10``

``hive.s3.max-client-retries``        Maximum number of read attempts to retry.                   ``5``

``hive.s3.max-backoff-time``          Use exponential backoff starting at 1 second up to          ``10 minutes``
                                      this maximum value when communicating with S3.

``hive.s3.max-retry-time``            Maximum time to retry communicating with S3.                ``10 minutes``

``hive.s3.connect-timeout``           TCP connect timeout.                                        ``5 seconds``

``hive.s3.socket-timeout``            TCP socket read timeout.                                    ``5 seconds``

``hive.s3.max-connections``           Maximum number of simultaneous open connections to S3.      ``500``

``hive.s3.multipart.min-file-size``   Minimum file size before multi-part upload to S3 is used.   ``16 MB``

``hive.s3.multipart.min-part-size``   Minimum multi-part upload part size.                        ``5 MB``
===================================== =========================================================== ===============

S3 Data Encryption
^^^^^^^^^^^^^^^^^^

Presto supports reading and writing encrypted data in S3 using both
server-side encryption with S3 managed keys and client-side encryption using
either the Amazon KMS or a software plugin to manage AES encryption keys.

With `S3 server-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html>`_,
(called *SSE-S3* in the Amazon documentation) the S3 infrastructure takes care of all encryption and decryption
work (with the exception of SSL to the client, assuming you have ``hive.s3.ssl.enabled`` set to ``true``).
S3 also manages all the encryption keys for you. To enable this, set ``hive.s3.sse.enabled`` to ``true``.

With `S3 client-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html>`_,
S3 stores encrypted data and the encryption keys are managed outside of the S3 infrastructure. Data is encrypted
and decrypted by Presto instead of in the S3 infrastructure. In this case, encryption keys can be managed
either by using the AWS KMS or your own key management system. To use the AWS KMS for key management, set
``hive.s3.kms-key-id`` to the UUID of a KMS key. Your AWS credentials or EC2 IAM role will need to be
granted permission to use the given key as well.

To use a custom encryption key management system, set ``hive.s3.encryption-materials-provider`` to the
fully qualified name of a class which implements the
`EncryptionMaterialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/EncryptionMaterialsProvider.html>`_
interface from the AWS Java SDK. This class will have to be accessible to the Hive Connector through the
classpath and must be able to communicate with your custom key management system. If this class also implements
the ``org.apache.hadoop.conf.Configurable`` interface from the Hadoop Java API, then the Hadoop configuration
will be passed in after the object instance is created and before it is asked to provision or retrieve any
encryption keys.

S3SelectPushdown
^^^^^^^^^^^^^^^^

S3SelectPushdown enables pushing down projection (SELECT) and predicate (WHERE)
processing to `S3 Select <https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html>`_.
With S3SelectPushdown Presto only retrieves the required data from S3 instead of
entire S3 objects reducing both latency and network usage.

Is S3 Select a good fit for my workload?
########################################

Performance of S3SelectPushdown depends on the amount of data filtered by the
query. Filtering a large number of rows should result in better performance. If
the query doesn't filter any data then pushdown may not add any additional value
and user will be charged for S3 Select requests. Thus, we recommend that you
benchmark your workloads with and without S3 Select to see if using it may be
suitable for your workload. By default, S3SelectPushdown is disabled and you
should enable it in production after proper benchmarking and cost analysis. For
more information on S3 Select request cost, please see
`Amazon S3 Cloud Storage Pricing <https://aws.amazon.com/s3/pricing/>`_.

Use the following guidelines to determine if S3 Select is a good fit for your
workload:

* Your query filters out more than half of the original data set.
* Your query filter predicates use columns that have a data type supported by
  Presto and S3 Select.
  The ``TIMESTAMP``, ``REAL``, and ``DOUBLE`` data types are not supported by S3
  Select Pushdown. We recommend using the decimal data type for numerical data.
  For more information about supported data types for S3 Select, see the
  `Data Types documentation <https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-data-types.html>`_.
* Your network connection between Amazon S3 and the Amazon EMR cluster has good
  transfer speed and available bandwidth. Amazon S3 Select does not compress
  HTTP responses, so the response size may increase for compressed input files.

Considerations and Limitations
##############################

* Only objects stored in CSV format are supported. Objects can be uncompressed
  or optionally compressed with gzip or bzip2.
* The "AllowQuotedRecordDelimiters" property is not supported. If this property
  is specified, the query fails.
* Amazon S3 server-side encryption with customer-provided encryption keys
  (SSE-C) and client-side encryption are not supported.
* S3 Select Pushdown is not a substitute for using columnar or compressed file
  formats such as ORC and Parquet.

Enabling S3 Select Pushdown
###########################

You can enable S3 Select Pushdown using the ``s3_select_pushdown_enabled``
Hive session property or using the ``hive.s3select-pushdown.enabled``
configuration property. The session property will override the config
property, allowing you enable or disable on a per-query basis.

Understanding and Tuning the Maximum Connections
################################################

Presto can use its native S3 file system or EMRFS. When using the native FS, the
maximum connections is configured via the ``hive.s3.max-connections``
configuration property. When using EMRFS, the maximum connections is configured
via the ``fs.s3.maxConnections`` Hadoop configuration property.

S3 Select Pushdown bypasses the file systems when accessing Amazon S3 for
predicate operations. In this case, the value of
``hive.s3select-pushdown.max-connections`` determines the maximum number of
client connections allowed for those operations from worker nodes.

If your workload experiences the error *Timeout waiting for connection from
pool*, increase the value of both ``hive.s3select-pushdown.max-connections`` and
the maximum connections configuration for the file system you are using.

Alluxio Configuration
---------------------

Presto can read and write tables stored in the Alluxio Data Orchestration System
`Alluxio <https://www.alluxio.io/?utm_source=prestodb&utm_medium=prestodocs>`_,
leveraging Alluxio's distributed block-level read/write caching functionality.
The tables must be created in the Hive metastore with the ``alluxio://`` location prefix
(see `Running Apache Hive with Alluxio <https://docs.alluxio.io/os/user/2.1/en/compute/Hive.html>`_
for details and examples).
Presto queries will then transparently retrieve and cache files
or objects from a variety of disparate storage systems including HDFS and S3.

Alluxio Client-Side Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To configure Alluxio client-side properties on Presto, append the Alluxio
configuration directory (``${ALLUXIO_HOME}/conf``) to the Presto JVM classpath,
so that the Alluxio properties file ``alluxio-site.properties`` can be loaded as a resource.
Update the Presto :ref:`presto_jvm_config` file ``etc/jvm.config`` to include the following:

.. code-block:: none

  -Xbootclasspath/a:<path-to-alluxio-conf>

The advantage of this approach is that all the Alluxio properties are set in
the single ``alluxio-site.properties`` file. For details, see `Customize Alluxio User Properties
<https://docs.alluxio.io/os/user/2.1/en/compute/Presto.html#customize-alluxio-user-properties>`_.

Alternatively, add Alluxio configuration properties to the Hadoop configuration
files (``core-site.xml``, ``hdfs-site.xml``) and configure the Hive connector
to use the `Hadoop configuration files <#hdfs-configuration>`__ via the
``hive.config.resources`` connector property.

Deploy Alluxio with Presto
^^^^^^^^^^^^^^^^^^^^^^^^^^

To achieve the best performance running Presto on Alluxio, it is recommended
to collocate Presto workers with Alluxio workers. This allows reads and writes
to bypass the network. See `Performance Tuning Tips for Presto with Alluxio
<https://www.alluxio.io/blog/top-5-performance-tuning-tips-for-running-presto-on-alluxio-1/?utm_source=prestodb&utm_medium=prestodocs>`_
for more details.

Alluxio Catalog Service
^^^^^^^^^^^^^^^^^^^^^^^

An alternative way for Presto to interact with Alluxio is via the
`Alluxio Catalog Service. <https://docs.alluxio.io/os/user/stable/en/core-services/Catalog.html?utm_source=prestodb&utm_medium=prestodocs>`_.
The primary benefits for using the Alluxio Catalog Service are simpler
deployment of Alluxio with Presto, and enabling schema-aware optimizations
such as transparent caching and transformations. Currently, the catalog service
supports read-only workloads.

The Alluxio Catalog Service is a metastore that can cache the information
from different underlying metastores. It currently supports the Hive metastore
as an underlying metastore. In for the Alluxio Catalog to manage the metadata
of other existing metastores, the other metastores must be "attached" to the
Alluxio catalog. To attach an existing Hive metastore to the Alluxio
Catalog, simply use the
`Alluxio CLI attachdb command <https://docs.alluxio.io/os/user/stable/en/operation/User-CLI.html#attachdb?utm_source=prestodb&utm_medium=prestodocs>`_.
The appropriate Hive metastore location and Hive database name need to be
provided.

.. code-block:: none

    ./bin/alluxio table attachdb hive thrift://HOSTNAME:9083 hive_db_name

Once a metastore is attached, the Alluxio Catalog can manage and serve the
information to Presto. To configure the Hive connector for Alluxio
Catalog Service, simply configure the connector to use the Alluxio
metastore type, and provide the location to the Alluxio cluster.
For example, your ``etc/catalog/catalog_alluxio.properties`` will include
the following (replace the Alluxio address with the appropriate location):

.. code-block:: none

    connector.name=hive-hadoop2
    hive.metastore=alluxio
    hive.metastore.alluxio.master.address=HOSTNAME:PORT

Now, Presto queries can take advantage of the Alluxio Catalog Service, such as
transparent caching and transparent transformations, without any modifications
to existing Hive metastore deployments.

Table Statistics
----------------

The Hive connector automatically collects basic statistics
(``numFiles', ``numRows``, ``rawDataSize``, ``totalSize``)
on ``INSERT`` and ``CREATE TABLE AS`` operations.

The Hive connector can also collect column level statistics:

============= ====================================================================
Column Type   Collectible Statistics
============= ====================================================================
``TINYINT``   number of nulls, number of distinct values, min/max values
``SMALLINT``  number of nulls, number of distinct values, min/max values
``INTEGER``   number of nulls, number of distinct values, min/max values
``BIGINT``    number of nulls, number of distinct values, min/max values
``DOUBLE``    number of nulls, number of distinct values, min/max values
``REAL``      number of nulls, number of distinct values, min/max values
``DECIMAL``   number of nulls, number of distinct values, min/max values
``DATE``      number of nulls, number of distinct values, min/max values
``TIMESTAMP`` number of nulls, number of distinct values, min/max values
``VARCHAR``   number of nulls, number of distinct values
``CHAR``      number of nulls, number of distinct values
``VARBINARY`` number of nulls
``BOOLEAN``   number of nulls, number of true/false values
============= ====================================================================

Automatic column level statistics collection on write is controlled by
the ``collect-column-statistics-on-write`` catalog session property.

.. _hive_analyze:

Collecting table and column statistics
--------------------------------------

The Hive connector supports collection of table and partition statistics
via the :doc:`/sql/analyze` statement. When analyzing a partitioned table,
the partitions to analyze can be specified via the optional ``partitions``
property, which is an array containing the values of the partition keys
in the order they are declared in the table schema::

    ANALYZE hive.sales WITH (
        partitions = ARRAY[
            ARRAY['partition1_value1', 'partition1_value2'],
            ARRAY['partition2_value1', 'partition2_value2']]);

This query will collect statistics for 2 partitions with keys:

* ``partition1_value1, partition1_value2``
* ``partition2_value1, partition2_value2``

Schema Evolution
----------------

Hive allows the partitions in a table to have a different schema than the
table. This occurs when the column types of a table are changed after
partitions already exist (that use the original column types). The Hive
connector supports this by allowing the same conversions as Hive:

* ``varchar`` to and from ``tinyint``, ``smallint``, ``integer`` and ``bigint``
* ``real`` to ``double``
* Widening conversions for integers, such as ``tinyint`` to ``smallint``

Any conversion failure will result in null, which is the same behavior
as Hive. For example, converting the string ``'foo'`` to a number,
or converting the string ``'1234'`` to a ``tinyint`` (which has a
maximum value of ``127``).

Avro Schema Evolution
---------------------

Presto supports querying and manipulating Hive tables with Avro storage format which has the schema set
based on an Avro schema file/literal. It is also possible to create tables in Presto which infers the schema
from a valid Avro schema file located locally or remotely in HDFS/Web server.

To specify that Avro schema should be used for interpreting table's data one must use ``avro_schema_url`` table property.
The schema can be placed remotely in
HDFS (e.g. ``avro_schema_url = 'hdfs://user/avro/schema/avro_data.avsc'``),
S3 (e.g. ``avro_schema_url = 's3n:///schema_bucket/schema/avro_data.avsc'``),
a web server (e.g. ``avro_schema_url = 'http://example.org/schema/avro_data.avsc'``)
as well as local file system. This url where the schema is located, must be accessible from the
Hive metastore and Presto coordinator/worker nodes.

The table created in Presto using ``avro_schema_url`` behaves the same way as a Hive table with ``avro.schema.url`` or ``avro.schema.literal`` set.

Example::

   CREATE TABLE hive.avro.avro_data (
      id bigint
    )
   WITH (
      format = 'AVRO',
      avro_schema_url = '/usr/local/avro_data.avsc'
   )

The columns listed in the DDL (``id`` in the above example) will be ignored if ``avro_schema_url`` is specified.
The table schema will match the schema in the Avro schema file. Before any read operation, the Avro schema is
accessed so query result reflects any changes in schema. Thus Presto takes advantage of Avro's backward compatibility abilities.

If the schema of the table changes in the Avro schema file, the new schema can still be used to read old data.
Newly added/renamed fields *must* have a default value in the Avro schema file.

The schema evolution behavior is as follows:

* Column added in new schema:
  Data created with an older schema will produce a *default* value when table is using the new schema.

* Column removed in new schema:
  Data created with an older schema will no longer output the data from the column that was removed.

* Column is renamed in the new schema:
  This is equivalent to removing the column and adding a new one, and data created with an older schema
  will produce a *default* value when table is using the new schema.

* Changing type of column in the new schema:
  If the type coercion is supported by Avro or the Hive connector, then the conversion happens.
  An error is thrown for incompatible types.

Limitations
^^^^^^^^^^^

The following operations are not supported when ``avro_schema_url`` is set:

* ``CREATE TABLE AS`` is not supported.
* Using partitioning(``partitioned_by``) or bucketing(``bucketed_by``) columns are not supported in ``CREATE TABLE``.
* ``ALTER TABLE`` commands modifying columns are not supported.

Procedures
----------

* ``system.create_empty_partition(schema_name, table_name, partition_columns, partition_values)``

    Create an empty partition in the specified table.

* ``system.sync_partition_metadata(schema_name, table_name, mode, case_sensitive)``

    Check and update partitions list in metastore. There are three modes available:

    * ``ADD`` : add any partitions that exist on the file system but not in the metastore.
    * ``DROP``: drop any partitions that exist in the metastore but not on the file system.
    * ``FULL``: perform both ``ADD`` and ``DROP``.

    The ``case_sensitive`` argument is optional. The default value is ``true`` for compatibility
    with Hive's ``MSCK REPAIR TABLE`` behavior, which expects the partition column names in
    file system paths to use lowercase (e.g. ``col_x=SomeValue``). Partitions on the file system
    not conforming to this convention are ignored, unless the argument is set to ``false``.

Extra Hidden Columns
----------

The Hive connector exposes extra hidden metadata columns in Hive tables. You can query these
columns as a part of SQL query like any other columns of the table.

* ``$path`` : Filepath for the given row data
* ``$file_size`` : Filesize for the given row
* ``$file_modified_time`` : Last file modified time for the given row

Examples
--------

The Hive connector supports querying and manipulating Hive tables and schemas
(databases). While some uncommon operations will need to be performed using
Hive directly, most operations can be performed using Presto.

Create a new Hive schema named ``web`` that will store tables in an
S3 bucket named ``my-bucket``::

    CREATE SCHEMA hive.web
    WITH (location = 's3://my-bucket/')

Create a new Hive table named ``page_views`` in the ``web`` schema
that is stored using the ORC file format, partitioned by date and
country, and bucketed by user into ``50`` buckets (note that Hive
requires the partition columns to be the last columns in the table)::

    CREATE TABLE hive.web.page_views (
      view_time timestamp,
      user_id bigint,
      page_url varchar,
      ds date,
      country varchar
    )
    WITH (
      format = 'ORC',
      partitioned_by = ARRAY['ds', 'country'],
      bucketed_by = ARRAY['user_id'],
      bucket_count = 50
    )

Drop a partition from the ``page_views`` table::

    DELETE FROM hive.web.page_views
    WHERE ds = DATE '2016-08-09'
      AND country = 'US'

Add an empty partition to the ``page_views`` table::

    CALL system.create_empty_partition(
        schema_name => 'web',
        table_name => 'page_views',
        partition_columns => ARRAY['ds', 'country'],
        partition_values => ARRAY['2016-08-09', 'US']);

Query the ``page_views`` table::

    SELECT * FROM hive.web.page_views

List the partitions of the ``page_views`` table::

    SELECT * FROM hive.web."page_views$partitions"

Create an external Hive table named ``request_logs`` that points at
existing data in S3::

    CREATE TABLE hive.web.request_logs (
      request_time timestamp,
      url varchar,
      ip varchar,
      user_agent varchar
    )
    WITH (
      format = 'TEXTFILE',
      external_location = 's3://my-bucket/data/logs/'
    )

Drop the external table ``request_logs``. This only drops the metadata
for the table. The referenced data directory is not deleted::

    DROP TABLE hive.web.request_logs

Drop a schema::

    DROP SCHEMA hive.web

Hive Connector Limitations
--------------------------

:doc:`/sql/delete` is only supported if the ``WHERE`` clause matches entire partitions.
