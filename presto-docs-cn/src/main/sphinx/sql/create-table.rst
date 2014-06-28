============
创建表
============

概要
--------

.. code-block:: none

    CREATE TABLE table_name AS query

详细介绍
-----------

创建一个新的表包含 :doc:`select` 的结果。

例子
--------

创建一个用来 ``orders`` 总和的表 ``orders_by_date`` ::

    CREATE TABLE orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

