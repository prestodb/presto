============
TPCH Catalog
============

The TPCH catalog provides a set of schemas to support the TPC
Benchmark™H (TPC-H). TPC-H is a database benchmark used to measure the
performance of highly-complex decision support databases.

This catalog can also be used to test the capabilities and query
syntax of Presto without configuring access to a real data
source. When you query a schema in the TPCH catalog, the catalog is
generating random data as it is needed.

Configuration
-------------

To configure the TPCH connector create a properties file named
``tpch.properties`` in the ``etc/catalog`` directory with the
following content:

.. code-block:: none
    
    connector.name=tpch

Using a TPCH Schema
-------------------

The TPCH catalog supplies several schemas. To get a list of TPCH
schemas, run the following command:

.. code-block:: none

    presto:jmx> use catalog tpch;
    presto:jmx> show schemas;
           Schema       
    --------------------
     information_schema 
     sf1                
     sf100              
     sf1000             
     sf10000            
     sf100000           
     sf300              
     sf3000             
     sf30000            
     sys                
     tiny               
    (11 rows)

Each schema displayed in the list of TPCH schemas shown above provides
the same set of tables with a different number of rows.
