===========
JDBC驱动
===========

Presto可以通过Java的JDBC访问，下载 :download:`jdbc` 并将JDBC加入到你程序的的class path中。
下面是支持的JDBC URL格式：

.. code-block:: none

    jdbc:presto://host:port
    jdbc:presto://host:port/catalog
    jdbc:presto://host:port/catalog/schema

例如，连接运行在 ``example.net`` 的 ``8080`` 端口上的Presto服务，
使用下面的URL访问catalog ``hive`` 和schema ``sales``：

.. code-block:: none

    jdbc:presto://example.net:8080/hive/sales
