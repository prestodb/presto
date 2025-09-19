=============
Release 0.292
=============

**Highlights**
==============

* Improve error handling of ``INTERVAL DAY``, ``INTERVAL HOUR``, and ``INTERVAL SECOND`` operators when experiencing overflows. `#24353 <https://github.com/prestodb/presto/pull/24353>`_
* Improve presto router UI. `#24411 <https://github.com/prestodb/presto/pull/24411>`_
* Upgrade bootstrap to version 5. `#24167 <https://github.com/prestodb/presto/pull/24167>`_
* Add Java and Native Arrow Flight connector. `#24427 <https://github.com/prestodb/presto/pull/24427>`_
* Add a MySQL-compatible function ``bit_length`` that returns the count of bits for the given string. `#24531 <https://github.com/prestodb/presto/pull/24531>`_
* Add support to build Presto with JDK 17. `#24677 <https://github.com/prestodb/presto/pull/24677>`_
* Add the ability to canonicalize JSON output through session property ``canonicalized_json_extract``. `#24614 <https://github.com/prestodb/presto/pull/24614>`_
* Add support for native ORC reader. `#23037 <https://github.com/prestodb/presto/pull/23037>`_
* Improve ``task.max-drivers-per-task`` by setting the default value to use thread concurrency of the host. `#24642 <https://github.com/prestodb/presto/pull/24642>`_
* Fix a security bug when ``check_access_control_for_utlized_columns`` is true for queries that uses a ``WITH`` clause. Previously we would sometimes not check permissions for certain columns that were used in the query.  Now we will always check permissions for all columns used in the query. There are some corner cases for CTEs with the same name where we may check more columns than are used or fall back to checking all columns referenced in the query. `#24647 <https://github.com/prestodb/presto/pull/24647>`_
* Fix Parquet read failing for nested Decimal types. `#24440 <https://github.com/prestodb/presto/pull/24440>`_
* Add manifest file caching for deployments which use the Hive metastore. `#24481 <https://github.com/prestodb/presto/pull/24481>`_
* Add table property ``write.data.path`` to specify independent data write paths for Iceberg tables. `#24397 <https://github.com/prestodb/presto/pull/24397>`_
* Add support for Iceberg table sort orders. Tables can be created to add a list of `sorted_by` columns which will be used to order files written to the table. `#21977 <https://github.com/prestodb/presto/pull/21977>`_
* Add support for ``UPDATE`` SQL statements. `#24281 <https://github.com/prestodb/presto/pull/24281>`_
* Add configuration property ``tpcds.use-varchar-type`` to allow toggling of char columns to varchar columns. `#24406 <https://github.com/prestodb/presto/pull/24406>`_

**Details**
===========

General Changes
_______________
* Fix Hive ``UUID`` type parsing. `#24538 <https://github.com/prestodb/presto/pull/24538>`_
* Fix addition, subtraction, multiplication and division of ``INTERVAL YEAR MONTH`` values. `#24617 <https://github.com/prestodb/presto/pull/24617>`_
* Fix index error when a map column is passed into an unnest function by using the column analyzer to correctly map key and value output fields back to correct input expression. `#24789 <https://github.com/prestodb/presto/pull/24789>`_
* Fix silently returning incorrect results when trying to construct a TimestampWithTimeZone from a value that has a unix timestamp that is too large/small. `#24674 <https://github.com/prestodb/presto/pull/24674>`_
* Fix a potential block by making the number of task event loop configurable via a configuration file. `#24565 <https://github.com/prestodb/presto/pull/24565>`_
* Improve analysis of utilized columns in a query by exploring view definitions and checking the utilized columns of the underlying tables. `#24638 <https://github.com/prestodb/presto/pull/24638>`_
* Improve error handling of ``INTERVAL DAY``, ``INTERVAL HOUR``, and ``INTERVAL SECOND`` operators when experiencing overflows. `#24353 <https://github.com/prestodb/presto/pull/24353>`_
* Improve scheduling by using long instead of DataSize for critical path. `#24582 <https://github.com/prestodb/presto/pull/24582>`_
* Improve scheduling by using long instead of DateTime for critical path. `#24673 <https://github.com/prestodb/presto/pull/24673>`_
* Improve presto router UI. `#24411 <https://github.com/prestodb/presto/pull/24411>`_
* Improve how multiple operator stats are merged together. `#24414 <https://github.com/prestodb/presto/pull/24414>`_
* Improve metrics creation by refactoring local variables to a dedicated class. `#24414 <https://github.com/prestodb/presto/pull/24414>`_
* Improve efficiency of coordinator when running a large number of tasks, controlled by ``task.enable-event-loop``. `#24668 <https://github.com/prestodb/presto/pull/24668>`_
* Add :doc:`../troubleshoot` topic to the Presto documentation. `#24601 <https://github.com/prestodb/presto/pull/24601>`_
* Add Arrow Flight connector. `#24427 <https://github.com/prestodb/presto/pull/24427>`_
* Add a MySQL-compatible function ``bit_length`` that returns the count of bits for the given string. `#24531 <https://github.com/prestodb/presto/pull/24531>`_
* Add configuration property ``exclude-invalid-worker-session-properties``. `#23968 <https://github.com/prestodb/presto/pull/23968>`_
* Add documentation for file-based Hive metastore to :doc:`/installation/deployment`. `#24620 <https://github.com/prestodb/presto/pull/24620>`_
* Add documentation for the :doc:`/connector/base-arrow-flight`. `#24427 <https://github.com/prestodb/presto/pull/24427>`_
* Add pagesink for DELETES to support future use. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Add serialization for new types. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Add support to build Presto with JDK 17. `#24677 <https://github.com/prestodb/presto/pull/24677>`_
* Add a new optimizer rule to add exchanges below a combination of partial aggregation+ GroupId . Enabled with the boolean session property ``add_exchange_below_partial_aggregation_over_group_id``. `#24047 <https://github.com/prestodb/presto/pull/24047>`_
* Add module presto-native-tests to run end-to-end tests with Presto native workers. `#24234 <https://github.com/prestodb/presto/pull/24234>`_
* Add map of node ID to plan node to QueryCompletedEvent in the event listener interface. `#24590 <https://github.com/prestodb/presto/pull/24590>`_
* Add support for multiple query event listeners. `#24456 <https://github.com/prestodb/presto/pull/24456>`_
* Add ``spark.dynamic-presto-memory-pool-tuning-enabled`` configuration property to dynamically configure available Spark executor memory based on available container memory. `#24714 <https://github.com/prestodb/presto/pull/24714>`_
* Add the ability to canonicalize JSON output through session property ``canonicalized_json_extract``. `#24614 <https://github.com/prestodb/presto/pull/24614>`_
* Add the ability for a file-based Hive metastore to use HDFS/S3 location as warehouse dir. `#24660 <https://github.com/prestodb/presto/pull/24660>`_
* Remove org.apache.logging.log4j:log4j-api from root POM. `#24605 <https://github.com/prestodb/presto/pull/24605>`_
* Remove org.apache.logging.log4j:log4j-core from root POM. `#24605 <https://github.com/prestodb/presto/pull/24605>`_
* Upgrade bootstrap to version 5. `#24167 <https://github.com/prestodb/presto/pull/24167>`_
* Upgrade jQuery to version 3.7.1. `#24167 <https://github.com/prestodb/presto/pull/24167>`_

Prestissimo (Native Execution) Changes
______________________________________
* Add a native type manager. `#24179 <https://github.com/prestodb/presto/pull/24179>`_
* Add support for Apache Arrow Flight connectors `#24504 <https://github.com/prestodb/presto/pull/24504>`_
* Add Presto native shared arbitrator configuration properties:
    * ``shared-arbitrator.global-arbitration-abort-time-ratio``.
    * ``shared-arbitrator.global-arbitration-memory-reclaim-pct``.
    * ``shared-arbitrator.global-arbitration-without-spill``.
    * ``shared-arbitrator.memory-pool-abort-capacity-limit``.
    * ``shared-arbitrator.memory-pool-min-reclaim-bytes``.
    * ``shared-arbitrator.memory-reclaim-threads-hw-multiplier``.

    `#24720 <https://github.com/prestodb/presto/pull/24720>`_
* Add a type parameter for ``ConnectorDeleteTableHandle`` implementations to ``ConnectorProtocolTemplate``, along with support for (de)serialization of connector-specific types.  Existing native connector implementations defining ``ConnectorProtocolTemplate`` specializations must update their definitions to supply their specific type or use ``NotImplemented``. `#24721 <https://github.com/prestodb/presto/pull/24721>`_
* Add ``exchange.http-client.request-data-sizes-max-wait-sec`` to native system configs. `#24774 <https://github.com/prestodb/presto/pull/24774>`_
* Add ``spill-enabled``, ``join-spill-enabled``, ``aggregation-spill-enabled``, and ``order-by-spill-enabled`` to native system configs. `#24726 <https://github.com/prestodb/presto/pull/24726>`_
* Add new error code name ``MEMORY_ARBITRATION_FAILURE`` under error code ``INSUFFICIENT_RESOURCE``. `#24773 <https://github.com/prestodb/presto/pull/24773>`_
* Add a native function namespace manager. `#23358 <https://github.com/prestodb/presto/pull/23358>`_
* Add support for ORC reader. `#23037 <https://github.com/prestodb/presto/pull/23037>`_
* Add node pool type specification when reporting to the coordinator from a C++ worker. `#24569 <https://github.com/prestodb/presto/pull/24569>`_
* Improve ``task.max-drivers-per-task`` by setting the default value to use thread concurrency of the host. `#24642 <https://github.com/prestodb/presto/pull/24642>`_

Security Changes
________________
* Fix a security bug when ``check_access_control_for_utlized_columns`` is true for queries that uses a ``WITH`` clause. Previously we would sometimes not check permissions for certain columns that were used in the query.  Now we will always check permissions for all columns used in the query. There are some corner cases for CTEs with the same name where we may check more columns than are used or fall back to checking all columns referenced in the query. `#24647 <https://github.com/prestodb/presto/pull/24647>`_
* Remove reload4j dependency in response to `WS-2022-0467 <https://www.mend.io/vulnerability-database/WS-2022-0467>`_. `#24606 <https://github.com/prestodb/presto/pull/24606>`_
* Replace deprecated ``dagre-d3`` with ``dagre-d3-es`` in response to a high severity vulnerability `WS-2022-0322 <https://github.com/opensearch-project/OpenSearch-Dashboards/issues/2482>`_. `#24167 <https://github.com/prestodb/presto/pull/24167>`_
* Upgrade libthrift to 0.14.1 in response to `CVE-2020-13949 <https://github.com/advisories/GHSA-g2fg-mr77-6vrm>`_. `#24462 <https://github.com/prestodb/presto/pull/24462>`_
* Upgrade netty dependencies to version 4.1.115.Final in response to `CVE-2024-47535 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47535>`_. `#24586 <https://github.com/prestodb/presto/pull/24586>`_
* Upgrade prismJs to 1.30.0 in response to `CVE-2024-53382 <https://github.com/advisories/GHSA-x7hr-w5r2-h6wg>`_. `#24765 <https://github.com/prestodb/presto/pull/24765>`_
* Upgrade the errorprone dependency from version 2.28.0 to 2.36.0. `#24475 <https://github.com/prestodb/presto/pull/24475>`_
* Upgrade the io.grpc library from version 1.68.0 to 1.70.0 in response to `CVE-2024-7254 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-7254>`_, `CVE-2020-8908 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-8908>`_. `#24475 <https://github.com/prestodb/presto/pull/24475>`_
* Upgrade org.apache.logging.log4j:log4j-api from 2.17.1 to 2.24.3 in response to `CVE-2024-47554 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47554>`_. `#24507 <https://github.com/prestodb/presto/pull/24507>`_
* Upgrade org.apache.logging.log4j:log4j-core from 2.17.1 to 2.24.3 in response to `CVE-2024-47554 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47554>`_. `#24507 <https://github.com/prestodb/presto/pull/24507>`_
* Upgrade commons-text to 1.13.0 in response to `CVE-2024-47554 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-47554>`_. `#24467 <https://github.com/prestodb/presto/pull/24467>`_
* Upgrade okhttp to 4.12.0 in response to  `CVE-2023-3635 <https://github.com/advisories/GHSA-w33c-445m-f8w7>`_. `#24473 <https://github.com/prestodb/presto/pull/24473>`_
* Upgrade okio to 3.6.0 in response to `CVE-2023-3635 <https://github.com/advisories/GHSA-w33c-445m-f8w7>`_. `#24473 <https://github.com/prestodb/presto/pull/24473>`_
* Upgrade org.apache.calcite to 1.38.0 in response to `CVE-2023-2976 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2023-2976>`_. `#24706 <https://github.com/prestodb/presto/pull/24706>`_
* Upgrade org.apache.ratis to 3.1.3 in response to `CVE-2020-15250 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-15250>`_. `#24496 <https://github.com/prestodb/presto/pull/24496>`_
* Upgrade aws-java-sdk version to 1.12.782 in response to `CVE-2024-21634 <https://nvd.nist.gov/vuln/detail/cve-2024-21634>`_. `#24606 <https://github.com/prestodb/presto/pull/24606>`_
* Upgrade json-smart version to 2.5.2 in response to `CVE-2024-57699 <https://nvd.nist.gov/vuln/detail/CVE-2024-57699>`_. `#24631 <https://github.com/prestodb/presto/pull/24631>`_
* Upgrade the accumulo version to 1.10.1 in response to `CVE-2020-17533 <https://github.com/advisories/GHSA-grc3-8q8m-4j7c>`_. `#24438 <https://github.com/prestodb/presto/pull/24438>`_
* Upgrade the hive-dwrf version to 0.8.7 which involved upgrading snappy version to 0.5 in response to `CVE-2024-36124 <https://github.com/advisories/GHSA-8wh2-6qhj-h7j9>`_. `#24461 <https://github.com/prestodb/presto/pull/24461>`_

Elasticsearch Connector Changes
_______________________________
* Improve cryptographic protocol in response to `Weak SSL/TLS protocols should not be used <https://sonarqube.ow2.org/coding_rules?open=java%3AS4423&rule_key=java%3AS4423>`_. `#24474 <https://github.com/prestodb/presto/pull/24474>`_

Hive Connector Changes
______________________
* Fix Parquet read failing for nested Decimal types. `#24440 <https://github.com/prestodb/presto/pull/24440>`_
* Fix getting views for Hive metastore 2.3+. `#24466 <https://github.com/prestodb/presto/pull/24466>`_
* Add session property ``hive.stats_based_filter_reorder_disabled`` for disabling reader stats based filter reordering. `#24630 <https://github.com/prestodb/presto/pull/24630>`_
* Replace return type of beginDelete. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Rename session property ``hive.stats_based_filter_reorder_disabled`` to ``hive.native_stats_based_filter_reorder_disabled``. `#24637 <https://github.com/prestodb/presto/pull/24637>`_
* Update native ``HiveConnectorProtocol`` to supply ``NotImplemented`` for ``ConnectorDeleteTableHandle`` type. `#24721 <https://github.com/prestodb/presto/pull/24721>`_

Iceberg Connector Changes
_________________________
* Fix IcebergTableHandle implementation to work with new types used in begin/finishDelete. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Fix bug with missing statistics when the statistics file cache has a partial miss. `#24480 <https://github.com/prestodb/presto/pull/24480>`_
* Fix Iceberg date column filtering. `#24583 <https://github.com/prestodb/presto/pull/24583>`_
* Add ``read.split.target-size`` table property. `#24417 <https://github.com/prestodb/presto/pull/24417>`_
* Add ``target_split_size_bytes`` session property. `#24417 <https://github.com/prestodb/presto/pull/24417>`_
* Add a dedicated subclass of ``FileHiveMetastore`` for the Iceberg connector to capture and isolate the differences in behavior. `#24573 <https://github.com/prestodb/presto/pull/24573>`_
* Add connector configuration property ``iceberg.catalog.hadoop.warehouse.datadir`` for Hadoop catalog to specify root data write path for its new created tables. `#24397 <https://github.com/prestodb/presto/pull/24397>`_
* Add logic to Iceberg type converter for timestamp with timezone. `#23534 <https://github.com/prestodb/presto/pull/23534>`_
* Add manifest file caching for deployments which use the Hive metastore. `#24481 <https://github.com/prestodb/presto/pull/24481>`_
* Add support for the ``hive.affinity-scheduling-file-section-size`` configuration property and ``affinity_scheduling_file_section_size`` session property. `#24598 <https://github.com/prestodb/presto/pull/24598>`_
* Add support of ``renaming table`` for Iceberg connector when configured with ``HIVE`` file catalog. `#24312 <https://github.com/prestodb/presto/pull/24312>`_
* Add table property ``write.data.path`` to specify independent data write paths for Iceberg tables. `#24397 <https://github.com/prestodb/presto/pull/24397>`_
* Add support for Iceberg table sort orders. Tables can be created to add a list of `sorted_by` columns which will be used to order files written to the table. `#21977 <https://github.com/prestodb/presto/pull/21977>`_
* Add support for ``UPDATE`` SQL statements. `#24281 <https://github.com/prestodb/presto/pull/24281>`_
* Deprecate some table property names in favor of property names from the Iceberg library. See :doc:`/connector/iceberg`. `#24581 <https://github.com/prestodb/presto/pull/24581>`_
* Improve Iceberg queries by enabling manifest file caching by default. `#24481 <https://github.com/prestodb/presto/pull/24481>`_
* Update native ``IcebergConnectorProtocol`` to supply ``NotImplemented`` for ``ConnectorDeleteTableHandle`` type. `#24721 <https://github.com/prestodb/presto/pull/24721>`_

Kudu Connector Changes
______________________
* Replace return type of beginDelete. `#24528 <https://github.com/prestodb/presto/pull/24528>`_

TPC-DS Connector Changes
________________________
* Add configuration property ``tpcds.use-varchar-type`` to allow toggling of char columns to varchar columns. `#24406 <https://github.com/prestodb/presto/pull/24406>`_

SPI Changes
___________
* Fix query failures by setting ``REMOTE_BUFFER_CLOSE_FAILED`` as a retriable error. `#24808 <https://github.com/prestodb/presto/pull/24808>`_
* Add ConnectorSession as an argument to PlanChecker.validate and PlanChecker.validateFragment. `#24557 <https://github.com/prestodb/presto/pull/24557>`_
* Add DeleteTableHandle support for the ConnectorTableHandles changes in Metadata. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Add ``CoordinatorPlugin#getExpressionOptimizerFactories`` to customize expression evaluation in the Presto coordinator. `#24144 <https://github.com/prestodb/presto/pull/24144>`_
* Add a separate ConnectorDeleteTableHandle interface for ``ConnectorMetadata.beginDelete`` and ``ConnectorMetadata.finishDelete``, replacing the previous usage of ConnectorTableHandle. `#24528 <https://github.com/prestodb/presto/pull/24528>`_
* Add IndexSourceNode to the SPI. `#24678 <https://github.com/prestodb/presto/pull/24678>`_
* Update ``beginDelete`` to return new types, and ``finishDelete`` to accept new types in ``ConnectorMetadata``. `#24528 <https://github.com/prestodb/presto/pull/24528>`_

**Credits**
===========

Abe Varghese, Amit Dutta, Anant Aneja, Andrii Rosa, Arjun Gupta, Artem Selishchev, Bryan Cutler, Chandrashekhar Kumar Singh, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Dilli-Babu-Godari, Elbin Pallimalil, Eric Liu, Gary Helmling, Ge Gao, HeidiHan0000, Jalpreet Singh Nanda, Jialiang Tan, Jiaqi Zhang, Joe Giardino, Ke, Kevin Tang, Kevin Wilfong, Krishna Pai, Li Zhou, Mahadevuni Naveen Kumar, Mariam Almesfer, Matt Karrmann, Minhan Cao, Natasha Sehgal, Nicholas Ormrod, Nidhin Varghese, Nikhil Collooru, Nivin C S, Patrick Sullivan, Pradeep Vaka, Pramod Satya, Prashant Sharma, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Richard Barnes, Sagar Sumit, Sayari Mukherjee, Sergey Pershin, Shahad, Shahim Sharafudeen, Shakyan Kushwaha, Shang Ma, Shelton Cai, Steve Burnett, Swapnil, Timothy Meehan, Xiao Du, Xiaoxuan Meng, Yihong Wang, Ying, Yuanda (Yenda) Li, Zac Blanco, Zac Wen, aditi-pandit, ajay-kharat, auden-woolfson, dnskr, inf, jay.narale, librian415, namya28, shenh062326, sumi, vhsu14, wangd, wypb
