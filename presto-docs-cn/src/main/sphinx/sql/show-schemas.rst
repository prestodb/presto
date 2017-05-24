============
SHOW SCHEMAS
============

概要
--------

.. code-block:: none

    SHOW SCHEMAS [ FROM catalog ]

详细介绍
-----------

List the schemas in ``catalog`` or in the current catalog.

参数
----------

catalog

    Name of the catalog

例子
--------

.. code-block:: none

    presto:default> show schemas;
           Schema       
    --------------------
     information_schema 
     jmx                
     sys                
    (3 rows)
