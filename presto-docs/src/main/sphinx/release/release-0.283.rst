=============
Release 0.283
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix Queued Query Count JMX Metrics (:issue:`19929`). Expose the QueryManagerStats metrics under the ``com.facebook.presto.dispatcher:name=DispatchManager`` namespace.
* Fix issue while using the iceberg table property ``format_version`` when ``iceberg.catalog.type`` is hive (:issue:`19778`).
* Fix join output for CrossJoinWithOrFilterToInnerJoin optimizer rule. It will fix potential failures with the message "Not all left output variables are before right output variables" when ``rewrite_cross_join_or_to_inner_join`` is enabled.
* Fix join output in RandomizeNullKeyInOuterJoin rule. It will fix failures with the message "Not all left output variables are before right output variables" when ``randomize_outer_join_null_key`` is enabled.
* Fix join re-ordering issues observed when dynamic filtering is turned on.
* Improve performance of getting resource group metrics.
* Add a optimizer rule ``RemoveIdentityProjectionsBelowProjection`` to remove identity projections under project node.
* Add a session parameter ``use_broadcast_when_buildsize_small_probeside_unknown`` to choose join distribution type This session is default to false. When enabled, broadcast join will be chosen when one side of input is within broadcast limit and the other side is unknow.
* Add function :func:`any_keys_match`.
* Add function :func:`any_values_match`.
* Add option to add partial row number node for row number node with max count limit, enabled by session parameter ``add_partial_node_for_row_number_node_with_limit``.
* Add string functions :func:`starts_with` and :func:`ends_with`.
* Add support for broadcast join in Presto-on-Spark/Velox execution path.
* Add support for internal authentication using JWT. Can be configured using configs ``internal-communication.jwt.enabled=[true/false]`` and ``internal-communication.shared-secret=<shared-secret-value>``.
* Add support for worker isolation by configuring leaf and intermediate worker pools.
* Add timeout for HBO optimizer, timeout set by session parameter ``history_based_optimizer_timeout_limit``.
* Added new property ``native-execution-broadcast-base-path``, which is used to specify base path for temporary storage of broadcast data for presto-on-spark native execution.
* Remove support for Presto Server RPM.
* Remove ``Experimental`` prefix from Dynamic Filtering.
* Add support for Hive S3 configuration to Iceberg Hadoop and Nessie catalogs.

Hive Changes
____________
* Fix a bug where the ParquetWriter throws "Size is greater than maximum int value" error when the table is large.
* Add Prestissimo support to write Parquet table storage format.
* Add support for the TTL of Alluxio SDK cache.

JDBC Changes
____________
* Add cache support for JDBC metadata calls. This can be enabled by configuring parameter ``metadata-cache-ttl``, ``metadata-cache-refresh-interval`` and ``metadata-cache-size``.

Native Changes
______________
* Add TPC-DS tests for native execution based on Parquet files.

Presto on Spark Changes
_______________________
* Add optimization to switch the build and probe sides of a join at runtime when a query runs with adaptive execution. This optimizer can be enabled by setting the session property ``adaptive_join_side_switching_enabeld = true`` or configuration property ``optimizer.adaptive-join-side-switching-enabled = true``.

**Credits**
===========

Ajay George, Ali Parsaei, Amit Dutta, Anant Aneja, Ankur Pathela, Ann Rose Benny, Arin Mathew, Avinash Jain, Beinan, Bikramjeet Vig, Bin Fan, Chandrashekhar Kumar Singh, Chunxu Tang, Deepak Majeti, Dongsheng Wang, Eduard Tudenhoefner, Elliotte Rusty Harold, Facebook Community Bot, Ge Gao, Haritha Koloth, Hunter Madison, Ivan Sadikov, Jalpreet Singh Nanda (:imjalpreet), Jaromir Vanek, Jialiang Tan, Jiayan Wei, Jimmy Lu, Karteekmurthys, Ke, Linsong Wang, Lyublena Antova, Mahadevuni Naveen Kumar, Masha Basmanova, Maxim Korolyov, Melissa Guo, Miaojiang (MJ) Deng, Michael Shang, Miguel Blanco Godón, Mikhail Slavoshevskii, Nikhil Collooru, Pedro Eugenio Rocha Pedreira, Pramod, Pranjal Shankhdhar, Pratyush Verma, Rebecca Schlussel, Reetika Agrawal, Rohan Pednekar, Rohit Jain, Sergey Pershin, Shrinidhi Joshi, Sotirios Delimanolis, Sreeni Viswanadha, Sudheesh, Timothy Meehan, Wei He, Zac, abhiseksaikia, aditi-pandit, feilong-liu, frankobe, jaystarshot, pratyakshsharma, v-jizhang, wypb, xiaoxmeng, yingsu00
