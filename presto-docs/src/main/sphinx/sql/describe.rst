========
DESCRIBE
========

Synopsis
--------

.. code-block:: none

    DESCRIBE table_name

Description
-----------

``DESCRIBE`` is an alias for :doc:`show-columns`.

Parameters
----------

table_name

    The name of a table.

Examples
--------

.. code-block: sql

    presto:default> describe airline_origin_destination;
         Column      |  Type   | Null | Partition Key 
    -----------------+---------+------+---------------
     itinid          | varchar | true | false         
     mktid           | varchar | true | false         
     seqnum          | varchar | true | false         
     coupons         | varchar | true | false         
     year            | varchar | true | false         
     quarter         | varchar | true | false         
     origin          | varchar | true | false         
     originaptind    | varchar | true | false         
     origincitynum   | varchar | true | false         
    (9 rows)

    Query 20140207_203829_00007_8qchq, FINISHED, 2 nodes
    Splits: 2 total, 2 done (100.00%)
    0:18 [34 rows, 3.6KB] [1 rows/s, 208B/s]