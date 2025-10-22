============================
Hive Connector Configuration
============================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

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

For authentication-related configuration of the Hive Metastore Thrift service and HDFS,
see :doc:`/connector/hive/hive-security`.

Hive Configuration Properties
-----------------------------

======================================================== ============================================================ ============
Property Name                                            Description                                                  Default
======================================================== ============================================================ ============
``hive.metastore.uri``                                   The URI(s) of the Hive metastore to connect to using the
                                                         Thrift protocol. If multiple URIs are provided, the first
                                                         URI is used by default and the rest of the URIs are
                                                         fallback metastores. This property is required.
                                                         Example: ``thrift://192.0.2.3:9083`` or
                                                         ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.metastore.username``                              The username Presto will use to access the Hive metastore.

``hive.config.resources``                                An optional comma-separated list of HDFS
                                                         configuration files. These files must exist on the
                                                         machines running Presto. Only specify this if
                                                         absolutely necessary to access HDFS.
                                                         Example: ``/etc/hdfs-site.xml``

``hive.storage-format``                                  The default file format used when creating new tables.       ``ORC``

``hive.compression-codec``                               The compression codec to use when writing files.             ``GZIP``

``hive.force-local-scheduling``                          Force splits to be scheduled on the same node as the Hadoop  ``false``
                                                         DataNode process serving the split data. This is useful for
                                                         installations where Presto is collocated with every
                                                         DataNode.

``hive.order-based-execution-enabled``                   Enable order-based execution. When enabled, Hive files       ``false``
                                                         become non-splittable and the table ordering properties
                                                         would be exposed to plan optimizer.

``hive.respect-table-format``                            Should new partitions be written using the existing table    ``true``
                                                         format or the default Presto format?

``hive.immutable-partitions``                            Can new data be inserted into existing partitions?           ``false``

``hive.create-empty-bucket-files``                       Should empty files be created for buckets that have no data? ``true``

``hive.max-partitions-per-writers``                      Maximum number of partitions per writer.                     100

``hive.max-partitions-per-scan``                         Maximum number of partitions for a single table scan.        100,000

``hive.dynamic-split-sizes-enabled``                     Enable dynamic sizing of splits based on data scanned by     ``false``
                                                         the query.

``hive.non-managed-table-writes-enabled``                Enable writes to non-managed (external) Hive tables.         ``false``

``hive.non-managed-table-creates-enabled``               Enable creating non-managed (external) Hive tables.          ``true``

``hive.collect-column-statistics-on-write``              Enables automatic column level statistics collection         ``false``
                                                         on write. See :doc:`/connector/hive/hive-table-statistics`
                                                         for details.

``hive.s3select-pushdown.enabled``                       Enable query pushdown to AWS S3 Select service.              ``false``

``hive.s3select-pushdown.max-connections``               Maximum number of simultaneously open connections to S3 for    500
                                                         S3SelectPushdown.

``hive.metastore.load-balancing-enabled``                Enable load balancing between multiple Metastore instances    ``false``

``hive.skip-empty-files``                                Enable skipping empty files. Otherwise, it will produce an   ``false``
                                                         error iterating through empty files.

``hive.file-status-cache.max-retained-size``             Maximum size in bytes of the directory listing cache          ``0KB``

``hive.metastore.catalog.name``                          Specifies the catalog name to be passed to the metastore.

``hive.experimental.symlink.optimized-reader.enabled``   Experimental: Enable optimized SymlinkTextInputFormat reader ``true``

``hive.copy-on-first-write-configuration-enabled``       Optimize the number of configuration copies by enabling       ``false``
                                                         copy-on-write technique.

                                                         CopyOnFirstWriteConfiguration acts as a wrapper around the
                                                         standard Hadoop Configuration object, extending its
                                                         behaviour by introducing an additional layer of
                                                         indirection. However, many third-party libraries that
                                                         integrate with Presto rely directly on the Configuration
                                                         copy `constructor`_. Since this constructor does not
                                                         recognise or account for the wrapped nature of
                                                         CopyOnFirstWriteConfiguration, it can result in silent
                                                         failures where critical configuration properties are not
                                                         correctly propagated.
                                                         
 ``hive.orc.use-column-names``                           Enable accessing ORC columns by name in the ORC file         ``false``
                                                         metadata, instead of their ordinal position. Also toggleable 
                                                         through the ``hive.orc_use_column_names`` session property.
======================================================== ============================================================ ============

.. _constructor: https://github.com/apache/hadoop/blob/02a9190af5f8264e25966a80c8f9ea9bb6677899/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/Configuration.java#L844-L875

Avro Configuration Properties
-----------------------------

When querying or creating Avro-formatted tables with the Hive connector, you may need to supply or override the Avro schema. In addition, Hive Metastore, especially Hive 3.x, must be configured to read storage schemas for Avro tables.

Table Properties
^^^^^^^^^^^^^^^^

These properties can be used when creating or querying Avro tables in Presto:

======================================================== ============================================================================== ======================================================================================
Property Name                                            Description                                                                    Default
======================================================== ============================================================================== ======================================================================================
``avro_schema_url``                                      URL or path (HDFS, S3, HTTP, or others) to the Avro schema file for             None (must be specified if Metastore does not provide or you need to
                                                         reading an Avro-formatted table. If specified, Presto will fetch                override schema)
                                                         and use this schema instead of relying on any schema in the
                                                         Metastore.
======================================================== ============================================================================== ======================================================================================

Hive Metastore Configuration for Avro
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To support Avro tables with schema properties when using Hive 3.x, you must configure the Hive Metastore service:

Add the ``metastore.storage.schema.reader.impl`` property to ``hive-site.xml`` where the metastore service is running:

.. code-block:: xml

    <property>
      <name>metastore.storage.schema.reader.impl</name>
      <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
    </property>

You must restart the metastore service for this configuration to take effect. This setting allows the metastore to read storage schemas for Avro tables and avoids ``Storage schema reading not supported`` errors.

Metastore Configuration Properties
----------------------------------

The required Hive metastore can be configured with a number of properties.

======================================================== ============================================================= ============
Property Name                                                         Description                                       Default
======================================================== ============================================================= ============
``hive.metastore-timeout``                               Timeout for Hive metastore requests.                           ``10s``

``hive.metastore-cache-ttl``                             Duration how long cached metastore data should be considered   ``0s``
                                                         valid.

``hive.metastore-cache-maximum-size``                    Hive metastore cache maximum size.                              10000

``hive.metastore-refresh-interval``                      Asynchronously refresh cached metastore data after access      ``0s``
                                                         if it is older than this but is not yet expired, allowing
                                                         subsequent accesses to see fresh data.

``hive.metastore-refresh-max-threads``                   Maximum threads used to refresh cached metastore data.          100

``hive.invalidate-metastore-cache-procedure-enabled``    When enabled, users will be able to invalidate metastore        false
                                                         cache on demand.

``hive.metastore.thrift.client.tls.enabled``             Whether TLS security is enabled.                                false

``hive.metastore.thrift.client.tls.keystore-path``       Path to the PEM or JKS key store.                               NONE

``hive.metastore.thrift.client.tls.keystore-password``   Password for the key store.                                     NONE

``hive.metastore.thrift.client.tls.truststore-path``     Path to the PEM or JKS trust store.                             NONE

``hive.metastore.thrift.client.tls.truststore-password`` Password for the trust store.                                   NONE

======================================================== ============================================================= ============

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
                                             (defaults to ``false``). This works with IAM roles in EC2.

                                              **Note:** This property is deprecated.

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

``hive.s3.web.identity.auth.enabled``        Enables Web Identity authentication for S3 access. Requires
                                             ``hive.s3.iam-role`` to be specified. Additionally, ensure that
                                             the environment variables ``AWS_WEB_IDENTITY_TOKEN_FILE`` and
                                             ``AWS_REGION`` are set for proper authentication. Since this
                                             implementation uses AWS SDK 1.x, setting these environment
                                             variables is necessary.
============================================ =================================================================

S3 Credentials
^^^^^^^^^^^^^^

If you are running Presto on Amazon EC2 using EMR or another facility,
it is recommended that you use IAM Roles for EC2 to govern access to S3. To enable this,
your EC2 instances will need to be assigned an IAM Role which grants appropriate
access to the data stored in the S3 bucket(s) you wish to use. It's also possible
to configure an IAM role with ``hive.s3.iam-role`` that will be assumed for accessing
any S3 bucket. This is much cleaner than setting AWS access and secret keys in the
``hive.s3.aws-access-key`` and ``hive.s3.aws-secret-key`` settings, and also allows
EC2 to automatically rotate credentials on a regular basis without any additional
work on your part.

After the introduction of DefaultAWSCredentialsProviderChain, if neither IAM role nor
IAM credentials are configured, instance credentials will be used as they are the last item
in the DefaultAWSCredentialsProviderChain.

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

AWS Security Mapping
^^^^^^^^^^^^^^^^^^^^

Presto supports flexible mapping for AWS Lake Formation and AWS S3 API calls, allowing for separate
credentials or IAM roles for specific users.

The mappings can be of two types: ``S3`` or ``LAKEFORMATION``.

The mapping entries are processed in the order listed in the configuration
file. More specific mappings should be specified before less specific mappings.
You can set default configuration by not including any match criteria for the last
entry in the list.

Each mapping entry when mapping type is ``S3`` may specify one match criteria. Available match criteria:

* ``user``: Regular expression to match against username. Example: ``alice|bob``

The mapping must provide one or more configuration settings:

* ``accessKey`` and ``secretKey``: AWS access key and secret key. This overrides
  any globally configured credentials, such as access key or instance credentials.

* ``iamRole``: IAM role to use. This overrides any globally configured IAM role.

Example JSON configuration file for s3:

.. code-block:: json

    {
      "mappings": [
        {
          "user": "admin",
          "accessKey": "AKIAxxxaccess",
          "secretKey": "iXbXxxxsecret"
        },
        {
          "user": "analyst|scientist",
          "iamRole": "arn:aws:iam::123456789101:role/analyst_and_scientist_role"
        },
        {
          "iamRole": "arn:aws:iam::123456789101:role/default"
        }
      ]
    }

Each mapping entry when mapping type is ``LAKEFORMATION`` may specify one match criteria. Available match criteria:

* ``user``: Regular expression to match against username. Example: ``alice|bob``

The mapping must provide one configuration setting:

* ``iamRole``: IAM role to use. This overrides any globally configured IAM role.

Example JSON configuration file for lakeformation:

.. code-block:: json

    {
      "mappings": [
        {
          "user": "admin",
          "iamRole": "arn:aws:iam::123456789101:role/admin_role"
        },
        {
          "user": "analyst",
          "iamRole": "arn:aws:iam::123456789101:role/analyst_role"
        },
        {
          "iamRole": "arn:aws:iam::123456789101:role/default_role"
        }
      ]
    }

======================================================= =================================================================
Property Name                                           Description
======================================================= =================================================================
``hive.aws.security-mapping.type``                      AWS Security Mapping Type. Possible values: S3 or LAKEFORMATION

``hive.aws.security-mapping.config-file``               JSON configuration file containing AWS IAM Security mappings

``hive.aws.security-mapping.refresh-period``            Time interval after which AWS IAM security mapping configuration
                                                        will be refreshed
======================================================= =================================================================

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
property, allowing you enable or disable on a per-query basis. Non-filtering
queries (``SELECT * FROM table``) are not pushed down to S3 Select,
as they retrieve the entire object content.

For uncompressed files, using supported formats and SerDes,
S3 Select scans ranges of bytes in parallel.
The scan range requests run across the byte ranges of the internal
Hive splits for the query fragments pushed down to S3 Select.
Parallelization is controlled by the existing ``hive.max-split-size``
property.

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