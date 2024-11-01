=============
Release 0.290
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix array_intersect for single parameter array<array<T>> to be deterministic regardless of the order of null input:pr:`23890`.
* Fix bug in local property calculation when spill is enabled :pr:`23922`.
* Fix bug to unescape like pattern and validate escape string with no unresolved value :pr:`23456`.
* Fix plan validation failures for some join queries running with spill enabled when using Presto C++.
* Fix to query and filter using Iceberg metadata columns "$path" and "$data_sequence_number" :pr:`23472`.
* Improve JoinPrefilter optimizer for wide join keys and multiple join keys :pr:`23858`.
* Improve writer scaling in skewed conditions by setting optimized_scale_writer_producer_buffer to on by default :pr:`23774`.
* Add UUID type support to the Parquet reader and writer. :pr:`23627`.
* Add a configurable-sized cache for Iceberg table puffin files to improve query planning time controlled by the  `iceberg.max-statistics-file-cache-size` configuration property. :pr:`23177`.
* Add a flag to the Presto CLI which allows skipping SSL certificate verification `:pr:23780`.
* Add a session property `native_max_extended_partial_aggregation_memory` which specifies presto native max partial aggregation memory when data reduction is optimal :pr:`23527`.
* Add a session property `native_max_partial_aggregation_memory` which specifies presto native max partial aggregation memory when data reduction is not optimal :pr:`23527`.
* Add a session property `native_max_spill_bytes ` which specifies presto native max allowed spill bytes :pr:`23527`.
* Add function :func: `is_private_ip` that returns true when the input IP address is private or a reserved IP address ... :pr:`23520`.
* Add function :func:`ip_prefix_subnets` that splits the input prefix into subnets the size of the new prefix length. ... :pr:`23656`.
* Add incremental periodic cache persistence for Presto C++ worker. :pr:`23626`.
* Add new property `eager-plan-validation-enabled` for eager building of validation of a logical plan before queuing. :pr:`23649`.
* Add presto icon to .idea :pr:`23579`.
* Add session property ``inline_projections_on_values`` and configuration property ``optimizer.inline-projections-on-values`` to evaluate project node on values node :pr:`23245`.
* Add support in QueuedStatement protocol to accept pre-minted query id and slug :pr:`23407`.
* Add support to proxy AuthorizedIdentity via JWT :pr:`23546`.
* Added support for casting char datatype to various numeric datatypes.     :pr:`23792`.
* Adds a new NodeManager : 'PluginNodeManager' :pr:`23863`.
* Replace configuration property `async-cache-full-persistence-interval` with `async-cache-persistence-interval`. :pr:`23626`.
* Remove array_dupes and array_has_dupes alias names from functions :func:`array_duplicates` and :func:`array_has_duplicates`.
* Avoid pushdown of negative position for element_at for array  :pr:`23479`.
* GET `/v1/info/state` to return INACTIVE state until the resource group configuration manager is fully initialized :pr:`23585`.
* Make IcebergDistributedSmokeTestBase abstract :pr:`23580`.
* Upgraded avro to version 1.11.4 :pr:`23868`.
* Upgraded commons-codec to version 1.17.0 :pr:`23868`.
* Upgraded commons-compress to version 1.26.2 :pr:`23868`.
* Upgraded commons-io to version 2.16.0 :pr:`23794`.
* Upgraded commons-io to version 2.16.1 :pr:`23868`.
* Upgraded commons-lang3 to version 3.14.0 :pr:`23868`.
* Upgraded protobuf-java to version 3.25.5 :pr:`23797`.
* Upgraded protobuf-java-util to version 3.25.5 :pr:`23797`.

Security Changes
________________
* Upgrade Postgres JDBC Driver to 42.6.1 :pr:`23710`.

Hive Connector Changes
______________________
* Fix interpretation of ambiguous timestamps inside array, map, or row types for tables using TEXTFILE format to interpret the timestamps as the earliest possible unixtime for consistency with the rest of Presto.
* Fix timestamps inside array, map, or row types for tables using TEXTFILE format to respect the ``hive.time-zone property``.

Iceberg Connector Changes
_________________________
* Fix time-type columns to return properly when ``iceberg.parquet-batch-read-optimization-enabled`` is set to ``TRUE``. :pr:`23542`.
* Fix to reduce drop time for Iceberg tables with deleted metadata in S3 storage. :pr:`23510`.
* Add Iceberg metadata table $ref :pr:`23503`.
* Add configuration property ``iceberg.rest.auth.oauth2.scope`` for OAUTH2 authentication in Iceberg's REST catalog :pr:`23884`.
* Add iceberg.rest.auth.oauth2.uri configurable property :pr:`23739`.
* Add procedure `rollback_to_timestamp` to rollback an iceberg table to a given point in time. :pr:`23559`.
* Add support of UUID-typed columns :pr:`23627`.
* Add support to query Iceberg table by branch/tag name :pr:`23539`.
* Add table property `metrics_max_inferred_column` to configure the max columns number for which metrics are collected, and support `metrics_max_inferred_column` for Iceberg table with `PARQUET` format :pr:`23468`.
* Support procedure fast_forward for iceberg :pr:`23589`.
* Support timestamp without timezone in time travel expressions :pr:`23714`.
* Support using named arguments in procedure `register_table` and `unregister_table` :pr:`12345`.

MongoDB Connector Changes
_________________________
* Support varbinary data type in MongoDB (pr:`23386`).

Mongodb Connector Changes
_________________________
* Add support for MongoDB ``ALTER TABLE`` statement. :pr:`23266`.

SPI Changes
___________
* Add ``Partitioning``, ``PartitioningScheme``, ``PartitioningHandle``, ``PlanFragmentId``, ``StageExecutionDescriptor`` and ``SimplePlanFragment`` to the SPI . :pr:`23601`.

Elasticsearch Changes
_____________________
* Improve handling of exceptions for empty tables in Elasticsearch :pr:`23850`.

Orc/dwrf Changes
________________
* Fix a data corruption in uncompressed ORC/DWRF files with large values in string/binary columns :pr:`23760`.

Prestissimo (native Execution) Changes
______________________________________
* Add ``native_max_local_exchange_partition_count session`` property. :pr:`23910`.

Presto Iceberg Changes
______________________
* Add test for logical type storage in parquet files :pr:`23388`.

Presto Parquet Changes
______________________
* Fix bug so that proper logical type parameters are now read and written to Parquet files :pr:`23388`.

**Credits**
===========

Abhisek Saikia, Amit Dutta, Anant Aneja, Ananthu-Nair, Andrii Rosa, Bikramjeet Vig, Bryan Cutler, Chen Yang, Christian Zentgraf, David Tolnay, Deepa-George, Deepak Majeti, Denodo Research Labs, Elbin Pallimalil, Elliotte Rusty Harold, Feilong Liu, Ge Gao, Hazmi, Jalpreet Singh Nanda (:imjalpreet), Jayaprakash Sivaprasad, Jialiang Tan, Jimmy Lu, Joe Abraham, Karnati-Naga-Vivek, Ke, Konjac Huang, Krishna Pai, Linsong Wang, Mahadevuni Naveen Kumar, Matt Calder, Naveen Nitturu, Nikhil Collooru, Pramod, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Richard Barnes, Rohan Pal Sidhu, Sam Partington, Serge Druzkin, Sergey Pershin, Steve Burnett, SthuthiGhosh9400, Swapnil Tailor, Timothy Meehan, Xiaoxuan Meng, Yihong Wang, Ying, Zac Blanco, Zac Wen, Zuyu ZHANG, abhibongale, aditi-pandit, ajay-kharat, auden-woolfson, exxiang, jackychen718, jaystarshot, kiersten-stokes, lingbin, lithinpurushothaman, lukmanulhakkeem, misterjpapa, mohsaka, namya28, oyeliseiev-ua, pratyakshsharma, prithvip, wangd
