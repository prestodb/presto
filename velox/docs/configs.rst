========================
Configuration properties
========================

Generic Configuration
---------------------
.. list-table::
   :widths: 20 10 10 70
   :header-rows: 1

   * - Property Name
     - Type
     - Default Value
     - Description
   * - preferred_output_batch_bytes
     - integer
     - 10MB
     - Preferred size of batches in bytes to be returned by operators from Operator::getOutput. It is used when an
       estimate of average row size is known. Otherwise preferred_output_batch_rows is used.
   * - preferred_output_batch_rows
     - integer
     - 1024
     - Preferred number of rows to be returned by operators from Operator::getOutput. It is used when an estimate of
       average row size is not known. When the estimate of average row size is known, preferred_output_batch_bytes is used.
   * - max_output_batch_rows
     - integer
     - 10000
     - Max number of rows that could be return by operators from Operator::getOutput. It is used when an estimate of
       average row size is known and preferred_output_batch_bytes is used to compute the number of output rows.

Memory Management
-----------------
.. list-table::
   :widths: 20 10 10 70
   :header-rows: 1

   * - Property Name
     - Type
     - Default Value
     - Description
   * - max_partial_aggregation_memory
     - integer
     - 16MB
     - Maximum amount of memory in bytes for partial aggregation results. Increasing this value can result in less
       network transfer and lower CPU utilization by allowing more groups to be kept locally before being flushed,
       at the cost of additional memory usage.
   * - max_extended_partial_aggregation_memory
     - integer
     - 16MB
     - Maximum amount of memory in bytes for partial aggregation results if cardinality reduction is below
       `partial_aggregation_reduction_ratio_threshold`. Every time partial aggregate results size reaches
       `max_partial_aggregation_memory` bytes, the results are flushed. If cardinality reduction is below
       `partial_aggregation_reduction_ratio_threshold`,
       i.e. `number of result rows / number of input rows > partial_aggregation_reduction_ratio_threshold`,
       memory limit for partial aggregation is automatically doubled up to `max_extended_partial_aggregation_memory`.
       This adaptation is disabled by default, since the value of `max_extended_partial_aggregation_memory` equals the
       value of `max_partial_aggregation_memory`. Specify higher value for `max_extended_partial_aggregation_memory` to enable.

Spilling
--------
.. list-table::
   :widths: 20 10 10 70
   :header-rows: 1

   * - Property Name
     - Type
     - Default Value
     - Description
   * - spill_enabled
     - boolean
     - false
     - Spill memory to disk to avoid exceeding memory limits for the query.
   * - spiller-spill-path
     - string
     - /tmp
     - Directory where spilled content is written.
   * - aggregation_spill_enabled
     - boolean
     - false
     - When `spill_enabled` is true, determines whether to spill memory to disk for aggregations to avoid exceeding
       memory limits for the query.
   * - join_spill_enabled
     - boolean
     - false
     - When `spill_enabled` is true, determines whether to spill memory to disk for hash joins to avoid exceeding memory
       limits for the query.
   * - order_by_spill_enabled
     - boolean
     - false
     - When `spill_enabled` is true, determines whether to spill memory to disk for order by to avoid exceeding memory
       limits for the query.
   * - aggregation_spill_memory_threshold
     - integer
     - 0
     - Maximum amount of memory in bytes that a final aggregation can use before spilling. 0 means unlimited.
   * - join_spill_memory_threshold
     - integer
     - 0
     - Maximum amount of memory in bytes that a hash join build side can use before spilling. 0 means unlimited.
   * - order_by_spill_memory_threshold
     - integer
     - 0
     - Maximum amount of memory in bytes that an order by can use before spilling. 0 means unlimited.
   * - spillable-reservation-growth-pct
     - integer
     - 25
     - The spillable memory reservation growth percentage of the current memory reservation size. Suppose a growth
       percentage of N and the current memory reservation size of M, the next memory reservation size will be
       M * (1 + N / 100). After growing the memory reservation K times, the memory reservation size will be
       M * (1 + N / 100) ^ K. Hence the memory reservation grows along a series of powers of (1 + N / 100).
       If the memory reservation fails, it starts spilling.
   * - max-spill-level
     - integer
     - 4
     - The maximum allowed spilling level with zero being the initial spilling level. Applies to hash join build
       spilling which might use recursive spilling when the build table is very large. -1 means unlimited.
       In this case an extremely large query might run out of spilling partition bits. The max spill level
       can be used to prevent a query from using too much io and cpu resources.
   * - max-spill-file-size
     - integer
     - 0
     - The maximum allowed spill file size. Zero means unlimited.
   * - min-spill-run-size
     - integer
     - 256MB
     - The minimum spill run size (bytes) limit used to select partitions for spilling. The spiller tries to spill a
       previously spilled partitions if its data size exceeds this limit, otherwise it spills the partition with most data.
       If the limit is zero, then the spiller always spills a previously spilled partition if it has any data. This is
       to avoid spill from a partition with a small amount of data which might result in generating too many small
       spilled files.

Hive Connector
--------------
.. list-table::
   :widths: 20 10 10 70
   :header-rows: 1

   * - Property Name
     - Type
     - Default Value
     - Description
   * - max_partitions_per_writers
     - integer
     - 100
     - Maximum number of partitions per a single table writer instance.
   * - insert_existing_partitions_behavior
     - string
     - ERROR
     - **Allowed values:** ``OVERWRITE``, ``ERROR``. The behavior on insert existing partitions. This property only derives
       the update mode field of the table writer operator output. ``OVERWRITE``
       sets the update mode to indicate overwriting a partition if exists. ``ERROR`` sets the update mode to indicate
       error throwing if writing to an existing partition.
   * - immutable_partitions
     - bool
     - false
     - True if appending data to an existing unpartitioned table is allowed. Currently this configuration does not
       support appending to existing partitions.

``Amazon S3 Configuration``
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. list-table::
   :widths: 30 10 10 70
   :header-rows: 1

   * - Property Name
     - Type
     - Default Value
     - Description
   * - hive.s3.use-instance-credentials
     - bool
     - true
     - Use the EC2 metadata service to retrieve API credentials. This works with IAM roles in EC2.
   * - hive.s3.aws-access-key
     - string
     -
     - Default AWS access key to use.
   * - hive.s3.aws-secret-key
     - string
     -
     - Default AWS secret key to use.
   * - hive.s3.endpoint
     - string
     -
     - The S3 storage endpoint server. This can be used to connect to an S3-compatible storage system instead of AWS.
   * - hive.s3.path-style-access
     - bool
     - false
     - Use path-style access for all requests to the S3-compatible storage. This is for S3-compatible storage that
       doesn’t support virtual-hosted-style access.
   * - hive.s3.ssl.enabled
     - bool
     - true
     - Use HTTPS to communicate with the S3 API.
   * - hive.s3.log-level
     - string
     - FATAL
     - **Allowed values:** "OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"
       Granularity of logging generated by the AWS C++ SDK library.
   * - hive.s3.iam-role
     - string
     -
     - IAM role to assume.
   * - hive.s3.iam-role-session-name
     - string
     - velox-session
     - Session name associated with the IAM role.

Spark-specific Configuration
----------------------------
.. list-table::
   :widths: 20 10 10 70
   :header-rows: 1

   * - Property Name
     - Type
     - Default Value
     - Description
   * - spark.legacy-size-of-null
     - bool
     - true
     - If false, ``size`` function returns null for null input.
