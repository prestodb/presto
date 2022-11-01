=============
Release 0.278
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix :func:`round` returning incorrect results for edge cases.
* Fix the compilation error when aggregation has order by clause and the input is a function.
* Add ``selectedUser`` into Identity object.
* Add a configuration property ``permissions.authorized-identity-selection-enable``. When it is true, the new API ``selectAuthorizedIdentity`` is called; otherwise, ``checkCanSetUser`` is called instead as before.
* Add a new security API ``selectAuthorizedIdentity``.
* Add an optimization which rewrites ``IF(predicate, AGG(x))`` to aggregation with mask in plan level. Controlled by system property `optimize_conditional_aggregation_enabled` and default to false.
* Add memory limit check for HashBuilderOperator during memory revoke.
* Add null masking for the Parquet decryption feature. When this feature is enabled and the user is denied access encrypted column, the columns will be removed in the requested schema sent to Parquet. Then it is filled out with 'NULL'  when the result is returned.
* Add optimization for `approx_percentile` functions evaluation. Multiple `approx_percentile` functions on the same field will be combined to be one `approx_percentile` function which takes an array of percentile as arguments. The optimization is controlled by session property `optimize_multiple_approx_percentile_on_same_field` which is true by default.
* Add optimization for outer join. Add randomized value for NULL join keys to avoid skew in NULL. Default to false.
* Added retry with increased partition count if query fails due to low partition count.
* Added session properties `spark_hash_partition_count_scaling_factor_on_out_of_memory` and `spark_retry_on_out_of_memory_higher_hash_partition_count_enabled` for this feature.
* A new builtin function :func:`map_subset` is added. This function takes a map and an array of keys and returns a map with entries from the input map with keys contained in the array supplied.
* Bump Apache Iceberg from 0.14.0 to 0.14.1.
* Upgrade Java Topology Suite (jts) library version to 1.19.0.

Delta Lake Connector Changes
____________________________
* Improve performance of reading newly created tables.

Filesystem Connector Changes
____________________________
* Add support for the HDFS filesystem connector in Presto Native Execution.

Tpc-h Connector Changes
_______________________
* Add support for the TPC-H connector in Presto Native Execution. Velox only supports standard column naming. The tpch connector property ``tpch.column-naming=standard`` must be set on the Java side.

SPI Changes
___________
* Move ``QueryType``, ``ErrorCode``, ``ErrorType`` from presto-spi to presto-common.

Delta Changes
_____________
* Add CREATE TABLE support to Delta connector.
* Add DROP TABLE support for the external table to Delta connector.

Hive Changes
____________
* Fix an issue which causes query failures when the Parquet file statistics is corrupted.
* A new session property - read_null_masked_parquet_encrypted_value_enabled is introduced. When this session property is enabled, the Parquet null masking feature is enabled. By default, it is disabled.

Hudi Changes
____________
* Upgrade the Apache Hudi version to 0.12.0.

Pinot Changes
_____________
* Fix Pinot ``BYTES`` type decoding issue.
* Add new config `pinot.query-options` and session property `pinot.query_options` to set [Pinot Query Options](https://docs.pinot.apache.org/users/user-guide-query/query-options) for generated Pinot query.
* Removed catalog config: `pinot.use-pinot-sql-for-broker-queries` and session config: `pinot.use_pinot_sql_for_broker_queries`.
* Removed unused configs: `pinot.allow-multiple-aggregations`, `pinot.thread-pool-size`, `pinot.min-connections-per-server`, `pinot.max-connections-per-server`, `pinot.max-backlog-per-server`, `pinot.idle-timeout`, `pinot.use-streaming-for-segment-queries`.
* Removed unused session configs: `pinot.ignore_empty_responses`, `pinot.connection_timeout`.
* Deprecated Pinot PQL query endpoint, by default using SQL query endpoint.
* Deprecated pinot netty server query.
* Support Pinot ``BigDecimal`` type.
* Upgrade Pinot release version to 0.11.0.

Router Changes
______________
* Add the weighted round-robin scheduling in the router.

**Credits**
===========

Aditi Pandit, Ahmed ElSherbiny, Ajay George, Amit Dutta, Amr Elroumy, Arjun Gupta, Arunachalam Thirupathi, Behnam Robatmili, Beinan, Chen Yang, Chunxu Tang, Deepak Majeti, Feilong Liu, Ge Gao, James Sun, Jimmy Lu, Karteek Murthy Samba Murthy, Krishna Pai, Lin Liu, MJ Deng, Masha Basmanova, Michael Shang, Milosz Linkiewicz, Naresh Kumar, Naveen Kumar Mahadevuni, Neerad Somanchi, Nizar Hejazi, Pranjal Shankhdhar, Rebecca Schlussel, Reetika Agrawal, Robert Stupp, Rohit Jain, Sacha Viscaino, Sagar Sumit, Sergey Pershin, Sergii Druzkin, Sreeni Viswanadha, Swapnil Tailor, Timothy Meehan, Todd Gao, Xiang Fu, Xinli Shang, Y Ethan Guo, abhiseksaikia, dnskr, pratyakshsharma, singcha, tanjialiang, xiaoxmeng, yingsu00
