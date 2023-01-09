=============
Release 0.269
=============

**Details**
===========

General Changes
_______________
* Add :func:`uuid` to return a unique identifier.

Presto on Spark Changes
_______________________
* Add configuration property ``spark.memory-revoking-target`` and session property ``spark_memory_revoking_target`` to specify the occupancy percent of memory pool after revoke is completed.

Accumulo Connector Changes
__________________________
* Replace :func:`uuid` from being connector specific to a standard function. In the process the return type has been changed from ``VARCHAR`` to ``UUID`` and existing use cases might need to perform a cast - ``CAST(UUID() AS VARCHAR)``.

Delta Lake Connector Changes
_____________________________
* Add :doc:`/connector/deltalake` for reading Delta Lake tables (https://delta.io/) natively in Presto.

Iceberg Connector Changes
_________________________
* Add Iceberg table predicate pushdown to reduce the number of files scanned.


**Credits**
===========

Ajay George, Andrii Rosa, Arjun Gupta, Arunachalam Thirupathi, Basil Crow, Beinan Wang, Costin V Cozianu, James Sun, Pranjal Shankhdhar, Sagar Sumit, Swapnil Tailor, Timothy Meehan, Venki Korukanti, Xiang Fu, Ying Su, Zhenxiao Luo, ericyuliu, sambekar15, singcha, v-jizhang
