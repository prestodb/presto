=============
Release 0.271
=============

**Details**
===========

General Changes
_______________
* Fix a bug where cache performance might be affected when ``CONSISTENT_HASHING`` is used as the scheduling strategy.
* Fix cast from bigint to varchar. Casting a number to a bounded varchar smaller than needed to hold the result will now fail.
  Example for presto CLI
    select cast(1234500000000000000 as varchar(3));

    Query ... failed: Value 1234500000000000000 cannot be represented as varchar(3)

* Fix reorder joins optimization where plan might not be optimal when original build side is larger than configured ``join-max-broadcast-table-size``.
* Add a new configuration property ``experimental.distinct-aggregation-large-block-size-threshold`` to define the threshold size beyond which the block will be spilled into a separate spill file.  This can be overridden by ``distinct_aggregation_large_block_size_threshold`` session property.
* Add a new configuration property ``experimental.distinct-aggregation-large-block-spill-enabled`` to enable spilling of blocks that are larger than ``experimental.distinct-aggregation-large-block-size-threshold`` bytes into a separate spill file.  This can be overridden by ``distinct_aggregation_large_block_spill_enabled`` session property.
* Add support for viewing expanded prepared query in Web UI.
* Generate a warning when creating a map with double/real as keys.

Hive Changes
____________
* Fix ANALYZE TABLE for partitioned Hive tables with complex columns (array, map, struct).
* Improve performance of ANALYZE TABLE on hive tables with complex columns.

Iceberg Changes
_______________
* Remove the iceberg.catalog.uri config. Use hive.metastore.uri instead.
* Add support for ORC format caching module for iceberg connector.
* Add support for basic timestamp in the iceberg connector.

**Credits**
===========

Abhishek Aggarwal, Amit Dutta, Arjun Gupta, Arunachalam Thirupathi, Beinan, Chunxu Tang, James Petty, James Sun, JySongWithZhangCe, Masha Basmanova, Mayank Garg, Neerad Somanchi, Otakar Trunecek, Pranjal Shankhdhar, Rebecca Schlussel, Rongrong Zhong, Sergii Druzkin, Shashwat Arghode, Swapnil Tailor, Timothy Meehan, Zitong Wei, Abhisek Saikia, Eric Liu, Mengdi Lin, singcha, v-jizhang
