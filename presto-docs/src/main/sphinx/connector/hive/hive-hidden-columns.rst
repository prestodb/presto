===================================
Hive Connector Extra Hidden Columns
===================================

The Hive connector exposes extra hidden metadata columns in Hive tables. Query these
columns as a part of the query like any other columns of the table.

* ``$path`` : Filepath for the given row data
* ``$file_size`` : Filesize for the given row (int64_t)
* ``$file_modified_time`` : Last file modified time for the given row (int64_t), in milliseconds since January 1, 1970 UTC
