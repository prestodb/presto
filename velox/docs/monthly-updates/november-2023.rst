********************
November 2023 Update
********************

Core Library
============

* Add spilling support for aggregations over distinct or sorted inputs. :pr:`7305`, :pr:`7526`
* Add support to lazily create the spill directory. :pr:`7660`
* Add config ``merge_exchange.max_buffer_size`` to limit the total memory used by exchange clients. :pr:`7410`
* Add configs ``sort_writer_max_output_rows`` and ``sort_writer_max_output_bytes`` to limit memory usage of sort writer. :pr:`7339`
* Add termination time to TaskStats. This is the time when the downstream workers finish consuming results. Clients such
  as Prestissimo can use this metric to clean up tasks. :pr:`7479`
* Add Presto Type Parser based on Flex and Bison. This can be used by a Presto verifier to parse types in the response
  from the Presto server :pr:`7568`
* Add support for named row fields in type signature and binding. Example: ``row(foo bigint)`` in a signature only
  binds to inputs whose type is row with a single BIGINT field named `foo`. :pr:`7523`
* Add support to shrink cache if clients such as Prestissimo detect high memory usage on a worker. :pr:`7547`, :pr:`7645`
* Fix distinct aggregations with global grouping sets on empty input. Instead of empty results, for global grouping sets,
  the expected result is a row per global grouping set with the groupId as the key value. :pr:`7353`
* Fix incorrect runtime stats reporting when memory arbitration is triggered. :pr:`7394`
* Fix ``Timestamp::toMillis()`` to overflow only if the final result overflows. :pr:`7506`

Presto Functions
================

* Add :func:`cosine_similarity` scalar function.
* Add support for INTERVAL DAY TO SECOND type input to :func:`plus`, :func:`minus`, :func:`multiply` functions.
* Add support for combination of TIMESTAMP, INTERVAL DAY TO SECOND type inputs to :func:`plus`, :func:`minus` functions.
* Add support for INTERVAL DAY TO SECOND, DOUBLE input arguments to :func:`divide` function.
* Add support to allow non-constant IN list in IN Presto predicate. :pr:`7497`
* Register :func:`array_frequency` function for all primitive types.
* Fix :doc:`bitwise shift functions</functions/presto/bitwise>` to accept shift value `0`.
* Fix :doc:`url_extract_*</functions/presto/url>` functions to return null on malformed inputs and support absolute URIs.
* Fix :func:`from_utf8` handling of invalid UTF-8 codepoint. :pr:`7442`
* Fix :func:`entropy` aggregate function to return `0.0` on null inputs.
* Fix :func:`array_sort` function from producing invalid dictionary vectors. :pr:`7800`
* Fix :func:`lead`, :func:`lag` window functions to return null when the offset is null. :pr:`7254`
* Fix DECIMAL to VARCHAR cast by adding trailing zeros when the value is `0`. :pr:`7588`

Spark Functions
===============

* Add :spark:func:`month`, :spark:func:`quarter`, :spark:func:`unscaled_value`, :spark:func:`regex_replace`
  scalar functions.
* Add :spark:func:`make_decimal`, :spark:func:`decimal_round` special form functions.
* Add support for DECIMAL compare with arguments of different precision and scale. :pr:`6207`
* Add support for complex type inputs to :spark:func:`map` function.
* Fix :spark:func:`dayofmonth` and :spark:func:`dayofyear` to allow only DATE type as input and return an INTEGER type.
* Fix :spark:func:`map` function from throwing an exception when used inside an if or switch statement. :pr:`7727`

Hive Connector
==============

* Add DirectBufferedInput: a selective BufferedInput without caching. :pr:`7217`
* Add support for reading UNSIGNED INTEGER types in Parquet format. :pr:`6728`
* Add spill support for DWRF sort writer. :pr:`7326`
* Add ``file_handle_cache_enabled`` :doc:`Hive Config</configs>` to enable or disable caching file handles.
* Add documentation for ``num_cached_file_handles`` :doc:`configuration property</configs>`.
* Add support for DECIMAL and VARCHAR types in BenchmarkParquetReader. :pr:`6275`

Arrow
=====

* Add support to export constant vector as `Arrow REE
  array <https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout>`_. :pr:`7327`, :pr:`7398`
* Add support for TIMESTAMP type in Arrow bridge. :pr:`7435`
* Fix Arrow bridge to ensure the null_count is always set and add support for null constants. :pr:`7411`

Performance and Correctness
===========================

* Add PrestoQueryRunner that can be used to verify test results against Presto. :pr:`7628`
* Add support for plans with TableScan in Join Fuzzer. :pr:`7571`
* Add support for custom input generators in Aggregation Fuzzer. :pr:`7594`
* Add support for aggregations over sorted inputs in AggregationFuzzer :pr:`7620`
* Add support for custom result verifiers in AggregationFuzzer. :pr:`7674`
* Add custom verifiers for :func:`approx_percentile` and :func:`approx_distinct` in AggregationFuzzer. :pr:`7654`
* Optimize map subscript by caching input keys in a hash map. :pr:`7191`
* Optimize `FlatVector<StringView>::copy()` slow path using a DecodedVector and pre-allocated the string buffer. :pr:`7357`
* Optimize `element_at` for maps with complex type keys by sorting the keys and using binary search. :pr:`7365`
* Optimize :func:`concat` by adding a fast path for primitive values. :pr:`7393`
* Optimize :func:`json_parse` function exception handling by switching to simdjson. :pr:`7658`
* Optimize :ref:`add_items<outputs-write>` for VARCHAR type by avoiding a deep copy. :pr:`7395`
* Optimize remaining filter by lazily evaluating multi-referenced fields. :pr:`7433`
* Optimize ``TopN::addInput()`` by deferring copying of the non-key columns. :pr:`7172`
* Optimize by sorting the inputs once when multiple aggregations share sorting keys and orders. :pr:`7452`
* Optimize Exchange operator by allowing merging of small batches of data into larger vectors. :pr:`7404`

Build
=====

* Add DuckDB version 0.8.1 as an external dependency and remove DuckDB amalgamation. :pr:`6725`
* Add `libcpr <https://github.com/libcpr/cpr>`_ a lightweight http client. :pr:`7385`
* Upgrade Arrow dependency to 14.0.1 from 13.0.0.

Credits
=======

Alex Hornby, Amit Dutta, Andrii Rosa, Austin Dickey Bikramjeet Vig, Cheng Huang, Chengcheng Jin, Christopher Ponce de Leon,
Daniel Munoz, Deepak Majeti, Ge Gao, Genevieve (Genna) Helsel, Harvey Hunt, Jake Jung, Jia, Jia Ke, Jialiang Tan,
Jimmy Lu, John Elliott, Karteekmurthys, Ke, Kevin Wilfong, Krishna Pai, Laith Sakka, Masha Basmanova, Orri Erling,
PHILO-HE, Patrick Sullivan, Pedro Eugenio Rocha Pedreira, Pramod, Richard Barnes, Schierbeck, Cody, Sergey Pershin,
Wei He, Zhenyuan Zhao, aditi-pandit, curt, duanmeng, joey.ljy, lingbin, rui-mo, usurai, vibhatha, wypb, xiaoxmeng,
xumingming, yangchuan, yaqi-zhao, yingsu00, yiweiHeOSS, youxiduo, zhli, 高阳阳