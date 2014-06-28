===============
描述表结构
===============

概要
--------

.. code-block:: none

    DESCRIBE table_name

详细介绍
-----------

``DESCRIBE`` 是 :doc:`show-columns` 的别名。

参数
----------

table_name

    表名

例子
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
