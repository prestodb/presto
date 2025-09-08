=============
Release 0.294
=============

**Highlights**
==============
* Improve query resource usage by enabling subfield pushdown for :func:`map_filter` when selected keys are constants. `#25451 <https://github.com/prestodb/presto/pull/25451>`_
* Improve query resource usage by enabling subfield pushdown for :func:`map_subset` when the input array is a constant array. `#25394 <https://github.com/prestodb/presto/pull/25394>`_
* Improve the efficiency of queries that involve with serialization operator by processing data in large group instead of one by one. `#25569 <https://github.com/prestodb/presto/pull/25569>`_
* Improve efficiency of queries with distinct aggregation and semi joins. `#25238 <https://github.com/prestodb/presto/pull/25238>`_
* Add changes to populate data source metadata to support combined lineage tracking. `#25127 <https://github.com/prestodb/presto/pull/25127>`_
* Add mixed case support for schema and table names. `#24551 <https://github.com/prestodb/presto/pull/24551>`_
* Add case-sensitive support for column names. It can be enabled for JDBC based connector by setting ``case-sensitive-name-matching=true`` at the catalog level. `#24983 <https://github.com/prestodb/presto/pull/24983>`_
* Update ``presto-plan-checker-router-plugin router`` plugin to use ``EXPLAIN (TYPE VALIDATE)`` in place of ``EXPLAIN (TYPE DISTRIBUTED)``, enabling faster routing of queries to either native or Java clusters. `#25545 <https://github.com/prestodb/presto/pull/25545>`_
* From release 0.294, due to Maven Central publishing limitations, executable jar files including ``presto-cli``, ``presto-benchmark-driver``, and ``presto-test-server-launcher`` are no longer published in the Maven Central repository. These jars can now be found on the `Presto GitHub release page <https://github.com/prestodb/presto/releases/tag/0.294>`_.

**Details**
===========

General Changes
_______________
* Fix filter pushdown to enable subfield pushdown for maps which are accessed with negative keys. `#25445 <https://github.com/prestodb/presto/pull/25445>`_
* Fix error classification for unsupported array comparison with null elements, converting it as a user error. `#25187 <https://github.com/prestodb/presto/pull/25187>`_
* Fix for :ref:`sql/update:UPDATE` statements involving multiple identical target column values. `#25599 <https://github.com/prestodb/presto/pull/25599>`_
* Fix inconsistent ordering with offset and limit. `#25216 <https://github.com/prestodb/presto/pull/25216>`_
* Fix precision loss in ``parse_duration`` function for large millisecond values. `#25538 <https://github.com/prestodb/presto/pull/25538>`_
* Fix randomize null join optimizer to keep HBO information for join input. `#25466 <https://github.com/prestodb/presto/pull/25466>`_
* Improve and optimize Docker image layers. `#25487 <https://github.com/prestodb/presto/pull/25487>`_
* Improve efficiency of inserts on ORC files. `#24913 <https://github.com/prestodb/presto/pull/24913>`_
* Improve query resource usage by enabling subfield pushdown for :func:`map_filter` when selected keys are constants. `#25451 <https://github.com/prestodb/presto/pull/25451>`_
* Improve query resource usage by enabling subfield pushdown for :func:`map_subset` when the input array is a constant array. `#25394 <https://github.com/prestodb/presto/pull/25394>`_
* Improve semi join performance for large filtering tables. `#25236 <https://github.com/prestodb/presto/pull/25236>`_
* Improve efficiency of queries with distinct aggregation and semi joins. `#25238 <https://github.com/prestodb/presto/pull/25238>`_
* Improve performance of min_by/max_by aggregations. `#25190 <https://github.com/prestodb/presto/pull/25190>`_
* Add :func:`dot_product(array(real), array(real)) -> real()` to calculate the sum of element wise product between two identically sized vectors represented as arrays. This function supports both array(real) and array(double) input types. For more information, refer to the `Dot Product definition <https://en.wikipedia.org/wiki/Dot_product>`_. `#25508 <https://github.com/prestodb/presto/pull/25508>`_
* Add ``broadcast_semi_join_for_delete`` session property to disable the ReplicateSemiJoinInDelete optimizer. `#25256 <https://github.com/prestodb/presto/pull/25256>`_
* Add ``history_based_optimizer_estimate_size_using_variables`` session property to have HBO estimate plan node output size using individual variables. `#25400 <https://github.com/prestodb/presto/pull/25400>`_
* Add changes to populate data source metadata to support combined lineage tracking. `#25127 <https://github.com/prestodb/presto/pull/25127>`_
* Add mixed case support for schema and table names. `#24551 <https://github.com/prestodb/presto/pull/24551>`_
* Add session property ``native_query_memory_reclaimer_priority`` which controls which queries are killed first when a worker is running low on memory. Higher value means lower priority to be consistent with Velox memory reclaimer's convention. See :doc:`/presto_cpp/properties-session`. `#25325 <https://github.com/prestodb/presto/pull/25325>`_
* Add xxhash64 override with seed argument. `#25521 <https://github.com/prestodb/presto/pull/25521>`_
* Add the :func:`l2_squared(array(real), array(real)) -> real()` function to Java workers. `#25409 <https://github.com/prestodb/presto/pull/25409>`_
* Update QueryPlanner to only include the optional ``$row_id`` column in :ref:`sql/delete:DELETE` query output variables when it is actually used by the connector. `#25284 <https://github.com/prestodb/presto/pull/25284>`_
* Update the default value of ``check_access_control_on_utilized_columns_only`` session property to ``true``. The ``false`` value makes the access check apply to all columns. See :ref:`admin/properties-session:\`\`check_access_control_on_utilized_columns_only\`\``. `#25469 <https://github.com/prestodb/presto/pull/25469>`_

Prestissimo (Native Execution) Changes
______________________________________
* Fix Native Plan Checker for CTAS and Insert queries. `#25115 <https://github.com/prestodb/presto/pull/25115>`_
* Fix native session property manager reading plugin configs from file. `#25553 <https://github.com/prestodb/presto/pull/25553>`_
* Fix PrestoExchangeSource 400 Bad Request by adding the "Host" header. `#25272 <https://github.com/prestodb/presto/pull/25272>`_
* Improve memory usage in the ``PartitionAndSerialize`` operator and lower memory usage when serializing a sort key. `#25393 <https://github.com/prestodb/presto/pull/25393>`_
* Improve the efficiency of queries that involve with serialization operator by processing data in large groups instead of one by one. `#25569 <https://github.com/prestodb/presto/pull/25569>`_
* Add geometry type to the list of supported types in NativeTypeManager. `#25560 <https://github.com/prestodb/presto/pull/25560>`_
* Update stats API and Presto UI to report number of drivers and splits separately. `#24671 <https://github.com/prestodb/presto/pull/24671>`_

Router Changes
______________
* Add the `Presto Plan Checker Router Scheduler Plugin <https://github.com/prestodb/presto/tree/master/presto-plan-checker-router-plugin/README.md>`_. `#25035 <https://github.com/prestodb/presto/pull/25035>`_
* Replace the parameters in router schedulers to use `RouterRequestInfo` to get the URL destination. `#25244 <https://github.com/prestodb/presto/pull/25244>`_
* Update ``presto-plan-checker-router-plugin router`` plugin to use ``EXPLAIN (TYPE VALIDATE)`` in place of ``EXPLAIN (TYPE DISTRIBUTED)``, enabling faster routing of queries to either native or Java clusters. `#25545 <https://github.com/prestodb/presto/pull/25545>`_
* Update router UI to eliminate vulnerabilities. `#25206 <https://github.com/prestodb/presto/pull/25206>`_

Security Changes
________________
* Add authorization support for ``SHOW CREATE TABLE``, ``SHOW CREATE VIEW``, ``SHOW COLUMNS``, and ``DESCRIBE`` queries. `#25364 <https://github.com/prestodb/presto/pull/25364>`_
* Upgrade ``commons-beanutils`` dependency to address `CVE-2025-48734 <https://github.com/advisories/GHSA-wxr5-93ph-8wr9>`_. `#25235 <https://github.com/prestodb/presto/pull/25235>`_
* Upgrade ``commons-lang3`` to 3.18.0 to address `CVE-2025-48924 <https://github.com/advisories/GHSA-j288-q9x7-2f5v>`_. `#25549 <https://github.com/prestodb/presto/pull/25549>`_
* Upgrade ``kafka`` to 3.9.1 in response to `CVE-2025-27817 <https://github.com/advisories/GHSA-vgq5-3255-v292>`_. `#25312 <https://github.com/prestodb/presto/pull/25312>`_

JDBC Driver Changes
___________________
* Fix issue introduced in `#25127 <https://github.com/prestodb/presto/pull/25127>`_ by introducing `TableLocationProvider` interface to decouple table location logic from JDBC configuration. `#25582 <https://github.com/prestodb/presto/pull/25582>`_
* Improve type mapping API to add WriteMapping functionality. `#25437 <https://github.com/prestodb/presto/pull/25437>`_
* Add mixed case support related catalog property in JDBC connector ``case-sensitive-name-matching``. `#24551 <https://github.com/prestodb/presto/pull/24551>`_
* Add case-sensitive support for column names. It can be enabled for JDBC based connector by setting ``case-sensitive-name-matching=true`` at the catalog level. `#24983 <https://github.com/prestodb/presto/pull/24983>`_

Arrow Flight Connector Changes
______________________________
* Add support for mTLS authentication in Arrow Flight client. See :ref:`connector/base-arrow-flight:Configuration`. `#25179 <https://github.com/prestodb/presto/pull/25179>`_

Delta Lake Connector Changes
____________________________
* Improve mapping of ``TIMESTAMP`` column type by changing it from Presto  ``TIMESTAMP`` type to ``TIMESTAMP_WITH_TIME_ZONE``. `#24418 <https://github.com/prestodb/presto/pull/24418>`_
* Add support for ``TIMESTAMP_NTZ`` column type as Presto ``TIMESTAMP`` type. ``legacy_timestamp`` should be set to ``false`` to match delta type specifications. When set to ``false``, ``TIMESTAMP`` will not adjust based on local timezone. `#24418 <https://github.com/prestodb/presto/pull/24418>`_

Hive Connector Changes
______________________
* Fix an issue while accessing symlink tables. `#25307 <https://github.com/prestodb/presto/pull/25307>`_
* Fix incorrectly ignoring computed table statistics in ``ANALYZE``. `#24973 <https://github.com/prestodb/presto/pull/24973>`_
* Improve split generation and read throughput for symlink tables. `#25277 <https://github.com/prestodb/presto/pull/25277>`_
* Add support for symlink files in :ref:`connector/hive:Quick Stats`. `#25250 <https://github.com/prestodb/presto/pull/25250>`_
* Update default value of ``hive.copy-on-first-write-configuration-enabled`` to ``false``. `#25420 <https://github.com/prestodb/presto/pull/25420>`_

Iceberg Connector Changes
_________________________
* Fix error querying ``$data_sequence_number`` metadata column for table with equality deletes. `#25293 <https://github.com/prestodb/presto/pull/25293>`_
* Fix the :ref:`connector/iceberg:Remove Orphan Files` procedure after deletion operations. `#25220 <https://github.com/prestodb/presto/pull/25220>`_
* Add ``iceberg.delete-as-join-rewrite-max-delete-columns`` configuration property and ``delete_as_join_rewrite_max_delete_columns`` session property to control when equality delete as join optimization is applied. The optimization is now only applied when the number of equality delete columns is less than or equal to this threshold (default: 400). Set to 0 to disable the optimization. See :doc:`/connector/iceberg`. `#25462 <https://github.com/prestodb/presto/pull/25462>`_
* Add support for ``$delete_file_path`` metadata column. `#25280 <https://github.com/prestodb/presto/pull/25280>`_
* Add support for ``$deleted`` metadata column. `#25280 <https://github.com/prestodb/presto/pull/25280>`_
* Add support of ``rename view`` for Iceberg connector when configured with ``REST`` and ``NESSIE``. `#25202 <https://github.com/prestodb/presto/pull/25202>`_
* Deprecate ``iceberg.delete-as-join-rewrite-enabled`` configuration property and ``delete_as_join_rewrite_enabled`` session property. Use ``iceberg.delete-as-join-rewrite-max-delete-columns`` instead. `#25462 <https://github.com/prestodb/presto/pull/25462>`_

MySQL Connector Changes
_______________________
* Add support for mixed-case in MySQL. It can be enabled by setting ``case-sensitive-name-matching=true`` configuration in the catalog configuration. `#24551 <https://github.com/prestodb/presto/pull/24551>`_

Redshift Connector Changes
__________________________
* Fix Redshift ``VARBYTE`` column handling for JDBC driver version 2.1.0.32+ by mapping ``jdbcType=1111`` and ``jdbcTypeName="binary varying"`` to Presto's ``VARBINARY`` type. `#25488 <https://github.com/prestodb/presto/pull/25488>`_
* Fix Redshift connector runtime failure due to a missing dependency on ``com.amazonaws.util.StringUtils``. Add ``aws-java-sdk-core`` as a runtime dependency to support Redshift JDBC driver (v2.1.0.32) which relies on this class for metadata operations. `#25265 <https://github.com/prestodb/presto/pull/25265>`_

SPI Changes
___________
* Add a function to SPI ``Constraint`` class to return the input arguments for the predicate. `#25248 <https://github.com/prestodb/presto/pull/25248>`_
* Add support for ``UnnestNode`` in connector optimizers. `#25317 <https://github.com/prestodb/presto/pull/25317>`_

Documentation Changes
_____________________
* Add :ref:`connector/hive:Avro Configuration Properties` to Hive Connector documentation. `#25311 <https://github.com/prestodb/presto/pull/25311>`_
* Add documentation for ``hive.copy-on-first-write-configuration-enabled`` configuration property to :ref:`connector/hive:Hive Configuration Properties`. `#25443 <https://github.com/prestodb/presto/pull/25443>`_

**Credits**
===========

Amit Dutta, Anant Aneja, Andrew Xie, Andrii Rosa, Auden Woolfson, Beinan, Chandra Vankayalapati, Chandrashekhar Kumar Singh, Chen Yang, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Elbin Pallimalil, Emily (Xuetong) Sun, Facebook Community Bot, Feilong Liu, Gary Helmling, Hazmi, HeidiHan0000, Henry Edwin Dikeman, Jalpreet Singh Nanda (:imjalpreet), Joe Abraham, Ke Wang, Ke Wang, Kevin Tang, Li Zhou, Mahadevuni Naveen Kumar, Natasha Sehgal, Nidhin Varghese, Nikhil Collooru, Nishitha-Bhaskaran, Ping Liu, Pradeep Vaka, Pramod Satya, Pratik Joseph Dabre, Raaghav Ravishankar, Rebecca Schlussel, Reetika Agrawal, Sebastiano Peluso, Sergey Pershin, Sergii Druzkin, Shahim Sharafudeen, Shakyan Kushwaha, Shang Ma, Shelton Cai, Shrinidhi Joshi, Soumya Duriseti, Sreeni Viswanadha, Steve Burnett, Thanzeel Hassan, Tim Meehan, Vincent Crabtree, Wei He, XiaoDu, Xiaoxuan, Yihong Wang, Ying, Zac Blanco, Zac Wen, Zhichen Xu, Zhiying Liang, Zoltan Arnold Nagy, aditi-pandit, ajay kharat, duhow, github username, jay.narale, lingbin, martinsander00, mohsaka, namya28, pratyakshsharma, vhsu14, wangd
