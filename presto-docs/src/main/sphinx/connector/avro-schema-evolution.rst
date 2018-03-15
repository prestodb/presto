===========================
Avro Schema Evolution Rules
===========================

Presto's Hive and Kafka connectors support Avro's schema evolution feature with backward compatibility. With backward compatibility,
a newer schema can be used to read Avro data created with an older schema. Newly added/renamed fields *must* have a default value in the Avro schema file.

The schema evolution behavior is as follows:

* Column added in new schema - Data created with an older schema will produce a “default” value when table is using the new schema.
* Column removed in new schema - Data created with an older schema will no longer output the data from the column that was removed.
* Column is renamed in the new schema - This is equivalent to removing the column and adding a new one, and data created with an older schema will produce a “default” value when table is using the new schema.
* Changing type of column in the new schema - If the type coercion is supported by Avro or at the connector level (eg: Hive), then the conversion happens. An error is thrown for incompatible types.
