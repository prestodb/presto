=============
Release 0.137
=============

General Changes
---------------

* Add :func:`bit_count`, :func:`bitwise_not`, :func:`bitwise_and`,
  :func:`bitwise_or`, and :func:`bitwise_xor` functions.
* Add :func:`approx_distinct` aggregation support for ``VARBINARY`` input.

Hive Changes
------------

* Add validation to :doc:`/sql/create-table` and :doc:`/sql/create-table-as`
  to check that partition keys are the last columns in the table and in the same 
  order as the table properties.