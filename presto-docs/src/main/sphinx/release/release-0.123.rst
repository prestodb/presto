=============
Release 0.123
=============

General Changes
---------------

* Remove ``node-scheduler.location-aware-scheduling-enabled`` config.

Table Properties
----------------

When creating tables with :doc:`/sql/create-table` or :doc:`/sql/create-table-as`,
you can now add connector specific properties to the new table.  For example, when
creating a Hive table you can specify the file format.  To list all available table
properties, run the following query::

    SELECT * FROM system.metadata.table_properties
