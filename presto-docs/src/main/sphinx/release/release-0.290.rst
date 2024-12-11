=============
Release 0.290
=============

**Highlights**
==============
* Fix to reduce drop time for Iceberg tables with deleted metadata in S3 storage. :pr:`23510`
* Fix a data corruption in uncompressed ORC/DWRF files with large values in string/binary columns. :pr:`23760`
* Improve JoinPrefilter optimizer for wide join keys and multiple join keys. :pr:`23858`
* Add UUID type support to the Parquet reader and writer. :pr:`23627`
* Add a configurable-sized cache for Iceberg table puffin files to improve query planning time controlled by the  ``iceberg.max-statistics-file-cache-size`` configuration property. :pr:`23177`
* Add support of UUID-typed columns. :pr:`23627`
* Add support to query Iceberg table by branch/tag name. :pr:`23539`
* Add support for procedure ``fast_forward`` for Iceberg. :pr:`23589`
* Add support for using named arguments in procedures ``register_table`` and ``unregister_table``. :pr:`23592`

**Details**
===========

General Changes
_______________
* Fix :func:`array_intersect` for single parameter array<array<T>> to be deterministic regardless of the order of null input. :pr:`23890`
* Fix bug in local property calculation when spill is enabled. :pr:`23922`
* Fix bug to unescape like pattern and validate escape string with no unresolved value. :pr:`23456`
* Fix to query and filter using Iceberg metadata columns ``$path`` and ``$data_sequence_number``. :pr:`23472`
* Fix nullability of columns in information schema. :pr:`23577`
* Fix distinct operator for UUID type. :pr:`23732`
* Improve ``element_at`` by avoiding pushdown of negative position for ``element_at`` for array.  :pr:`23479`
* Improve ``GET /v1/info/state`` to return INACTIVE state until the resource group configuration manager is fully initialized. :pr:`23585`
* Improve JoinPrefilter optimizer for wide join keys and multiple join keys. :pr:`23858`
* Improve writer scaling in skewed conditions by setting ``optimized_scale_writer_producer_buffer`` to ``on`` by default. :pr:`23774`
* Add UUID type support to the Parquet reader and writer. :pr:`23627`
* Add a configurable-sized cache for Iceberg table puffin files to improve query planning time controlled by the  ``iceberg.max-statistics-file-cache-size`` configuration property. :pr:`23177`
* Add a flag to the Presto CLI which allows skipping SSL certificate verification. :pr:`23780`
* Add a session property ``native_max_extended_partial_aggregation_memory`` which specifies Presto native max partial aggregation memory when data reduction is optimal. :pr:`23527`
* Add a session property ``native_max_partial_aggregation_memory`` which specifies Presto native max partial aggregation memory when data reduction is not optimal. :pr:`23527`
* Add a session property ``native_max_spill_bytes`` which specifies Presto native max allowed spill bytes. :pr:`23527`
* Add function :func:`is_private_ip` that returns true when the input IP address is private or a reserved IP address. :pr:`23520`
* Add function :func:`ip_prefix_subnets` that splits the input prefix into subnets the size of the new prefix length. :pr:`23656`
* Add new configuration property ``eager-plan-validation-enabled`` for eager building of validation of a logical plan before queuing. :pr:`23649`
* Add session property ``inline_projections_on_values`` and configuration property ``optimizer.inline-projections-on-values`` to evaluate project node on values node. :pr:`23245`
* Add support in QueuedStatement protocol to accept pre-minted query id and slug. :pr:`23407`
* Add support to proxy AuthorizedIdentity using JWT. :pr:`23546`
* Add support for casting ``char`` datatype to various numeric datatypes. :pr:`23792`
* Replace configuration property ``async-cache-full-persistence-interval`` with ``async-cache-persistence-interval``. :pr:`23626`
* Remove ``array_dupes`` and ``array_has_dupes`` alias names from functions :func:`array_duplicates` and :func:`array_has_duplicates`. :pr:`23762`

Presto C++ Changes
______________________________________
* Fix ``task.writer-count`` and ``task.partitioned-writer-count`` configuration properties in Presto C++ for consistency with Presto. :pr:`23902`
* Fix a bug where users weren't able to set the ``native_expression.max_array_size_in_reduce`` session property. :pr:`23856`
* Fix plan validation failures for some join queries running with spill enabled when using Presto C++. :pr:`23595`
* Fix bug so that proper logical type parameters are now read and written to Parquet files. :pr:`23388`
* Fix a data corruption in uncompressed ORC/DWRF files with large values in string/binary columns. :pr:`23760`
* Improve arbitrator configs to use the new string-based format. :pr:`23496`
* Add ``$path`` and ``$bucket`` to split info, and fixed the split counts in the coordinator UI. :pr:`23755`
* Add a metric ``presto_cpp.memory_pushback_expected_reduction_bytes`` to track expected reduction in memory after a pushback attempt. :pr:`23872`
* Add a new counter, ``presto_cpp.memory_pushback_reduction_bytes``, to monitor the actual memory reduction achieved with each memory pushback attempt. :pr:`23813`
* Add ``native_max_local_exchange_partition_count`` session property which maps to the ``max_local_exchange_partition_count`` velox query property to limit the number of partitions created by a local exchange. :pr:`23910`
* Add session property: ``native_writer_flush_threshold_bytes`` which specifies the minimum memory footprint size required to reclaim memory from a file writer by flushing its buffered data to disk. :pr:`23891`
* Add session property: ``native_max_page_partitioning_buffer_size`` which specifies the maximum bytes to buffer per PartitionedOutput operator to avoid creating tiny SerializedPages. :pr:`23853`
* Add session property: ``native_max_output_buffer_size`` which specifies the maximum size in bytes for the task's buffered output. The buffer is shared among all drivers. :pr:`23853`
* Add incremental periodic cache persistence for Presto C++ worker. :pr:`23626`
* Add native system session property provider. :pr:`23045`
* Remove session property ``native_join_spiller_partition_bits``. :pr:`23906`
* Revert merging of ``FilterNode`` into ``TableScanNode`` done in :pr:`23755`. :pr:`23855`

Security Changes
________________
* Upgrade Postgres JDBC Driver to 42.6.1 in response to `CVE-2024-1597 <https://nvd.nist.gov/vuln/detail/CVE-2024-1597>`_. :pr:`23710`
* Upgrade the logback-core version to 1.2.13 in response to `CVE-2023-6378 <https://github.com/advisories/GHSA-vmq6-5m68-f53m>`_. :pr:`23735`

Hive Connector Changes
______________________
* Fix interpretation of ambiguous timestamps inside array, map, or row types for tables using ``TEXTFILE`` format to interpret the timestamps as the earliest possible unixtime for consistency with the rest of Presto. :pr:`23593`
* Fix timestamps inside array, map, or row types for tables using ``TEXTFILE`` format to respect the ``hive.time-zone property``. :pr:`23593`

Iceberg Connector Changes
_________________________
* Fix time-type columns to return properly when ``iceberg.parquet-batch-read-optimization-enabled`` is set to ``TRUE``. :pr:`23542`
* Fix to reduce drop time for Iceberg tables with deleted metadata in S3 storage. :pr:`23510`
* Fix bug so that proper logical type parameters are now read and written to Parquet files. :pr:`23388`
* Fix a data corruption in uncompressed ORC/DWRF files with large values in string/binary columns. :pr:`23760`
* Add Iceberg metadata table ``$ref``. :pr:`23503`
* Add configuration property ``iceberg.rest.auth.oauth2.scope`` for OAUTH2 authentication in Iceberg's REST catalog. :pr:`23884`
* Add configuration property ``iceberg.rest.auth.oauth2.uri``. :pr:`23739`
* Add procedure ``rollback_to_timestamp`` to rollback an Iceberg table to a given point in time. :pr:`23559`
* Add support of UUID-typed columns. :pr:`23627`
* Add support to query Iceberg table by branch/tag name. :pr:`23539`
* Add table property ``metrics_max_inferred_column`` to configure the max columns number for which metrics are collected, and support ``metrics_max_inferred_column`` for Iceberg tables with `PARQUET` format. :pr:`23468`
* Add support for procedure ``fast_forward`` for Iceberg. :pr:`23589`
* Add support for using named arguments in procedures ``register_table`` and ``unregister_table``. :pr:`23592`
* Support new procedure ``set_current_snapshot`` for Iceberg. :pr:`23567`
* Support timestamp without timezone in time travel expressions. :pr:`23714`

MongoDB Connector Changes
_________________________
* Add support for ``varbinary`` data type in MongoDB. :pr:`23386`
* Add support for MongoDB ``ALTER TABLE`` statement. :pr:`23266`

Cassandra Connector Changes
___________________________
* Upgrade cassandra-driver-core to 3.11.5 for SSL support. :pr:`23493`

Elasticsearch Connector Changes
_______________________________
* Improve handling of exceptions for empty tables in Elasticsearch. :pr:`23850`

SPI Changes
___________
* Add ``Partitioning``, ``PartitioningScheme``, ``PartitioningHandle``, ``PlanFragmentId``, ``StageExecutionDescriptor`` and ``SimplePlanFragment`` to the SPI. :pr:`23601`

**Credits**
===========

Abhisek Saikia, Amit Dutta, Anant Aneja, Ananthu-Nair, Andrii Rosa, Bikramjeet Vig, Bryan Cutler, Chen Yang, Christian Zentgraf, David Tolnay, Deepa-George, Deepak Majeti, Denodo Research Labs, Elbin Pallimalil, Elliotte Rusty Harold, Feilong Liu, Ge Gao, Hazmi, Jalpreet Singh Nanda (:imjalpreet), Jayaprakash Sivaprasad, Jialiang Tan, Jimmy Lu, Joe Abraham, Karnati-Naga-Vivek, Ke, Konjac Huang, Krishna Pai, Linsong Wang, Mahadevuni Naveen Kumar, Matt Calder, Naveen Nitturu, Nikhil Collooru, Pramod, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Richard Barnes, Rohan Pal Sidhu, Sam Partington, Serge Druzkin, Sergey Pershin, Steve Burnett, SthuthiGhosh9400, Swapnil Tailor, Timothy Meehan, Xiaoxuan Meng, Yihong Wang, Ying, Zac Blanco, Zac Wen, Zuyu ZHANG, abhibongale, aditi-pandit, ajay-kharat, auden-woolfson, exxiang, jackychen718, jaystarshot, kiersten-stokes, lingbin, lithinpurushothaman, lukmanulhakkeem, misterjpapa, mohsaka, namya28, oyeliseiev-ua, pratyakshsharma, prithvip, wangd
