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

    The name of a table

Examples
--------

.. code-block:: none

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
