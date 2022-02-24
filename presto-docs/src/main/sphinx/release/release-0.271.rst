=============
Release 0.271
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix reorder joins optimization where plan might not be optimal when original build side is larger than configured ``join-max-broadcast-table-size``.
* Add a new configuration property ``experimental.distinct-aggregation-large-block-size-threshold`` to define the threshold size beyond which the block will be spilled into a separate spill file.  This can be overridden by ``distinct_aggregation_large_block_size_threshold`` session property.
* Add a new configuration property ``experimental.distinct-aggregation-large-block-spill-enabled`` to enable spilling of blocks that are larger than ``experimental.distinct-aggregation-large-block-size-threshold`` bytes into a separate spill file.  This can be overridden by ``distinct_aggregation_large_block_spill_enabled`` session property.
* Add support for viewing expanded prepared query in Web UI.
* Test and fix cast from bigint to varchar.

Hive Changes
____________
* Fix ANALYZE TABLE for partitioned Hive tables with complex columns (array, map, struct).
* Improve performance of ANALYZE TABLE on hive tables with complex columns.

Iceberg Changes
_______________
* Remove the iceberg.catalog.uri config. Use hive.metastore.uri instead.
* Support ORC format caching module for iceberg connector.
* Support basic timestamp in the iceberg connector.

**Credits**
===========

Abhishek Aggarwal, Amit Dutta, Arjun Gupta, Arunachalam Thirupathi, Beinan, Chunxu Tang, James Petty, James Sun, JySongWithZhangCe, Masha Basmanova, Mayank Garg, Neerad Somanchi, Otakar Trunecek, Pranjal Shankhdhar, Rebecca Schlussel, Rongrong Zhong, Sergii Druzkin, Shashwat Arghode, Swapnil Tailor, Timothy Meehan, Zitong Wei, abhiseksaikia, ericyuliu, mengdilin, singcha, v-jizhang
