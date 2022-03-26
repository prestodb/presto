=============
Release 0.267
=============

**Details**
===========

Google Sheet Changes
____________________
* Introduce Google sheet connector, supporting read queries.

Hive Changes
____________
* Add support for weighing splits according to their file size, allowing deeper worker split queues when files are small.
  This behavior is enabled by default, and can be disabled by setting the configuration property ``hive.size-based-split-weights-enabled``
  or the session property ``size_based_split_weights_enabled`` to false. Additionally, when enabled, the extent to which hive splits are considered "smaller" than standard
  can be controlled by using the configuration property ``hive.minimum-assigned-split-weight`` or session property ``minimum_assigned_split_weight``. The default value is 0.05.
* Add support for reading LZ4 and ZSTD compressed parquet data.
* Switch the default value of config ``hive.orc.writer.stream-layout-type`` to ``BY_COLUMN_SIZE`` to improve IO efficiency.

Pinot Changes
_____________
* Fix query crash when Pinot query returns mismatched schema.
* Add support to enable Pinot proxy for broker and grpc requests.
* Add support to pushdown ``IS NULL`` and ``IS NOT NULL`` queries to Pinot.
* Add support to run ``IS NOT NULL`` queries on Pinot data from Presto.

**Credits**
===========

Arunachalam Thirupathi, Beinan Wang, Chen, James Petty, James Sun, Pranjal Shankhdhar, Reetika Agrawal, Rongrong Zhong, Sreeni Viswanadha, Varun Gajjala, Xiang Fu, Xinli shang, Zac, Zhenxiao Luo, kriti-sc, tanjialiang, v-jizhang
