============
SHOW SCHEMAS
============

Synopsis
--------

.. code-block:: none

    SHOW SCHEMAS [ FROM catalog ]

Description
-----------

List the schemas in ``catalog`` or in the current catalog.

Parameters
----------

catalog

    Presto catalog name


Examples
--------

.. code-block:: sql

    presto:default> show schemas;
           Schema       
    --------------------
     information_schema 
     jmx                
     sys                
    (3 rows)

    Query 20140208_141910_00006_8qchq, FINISHED, 2 nodes
    Splits: 2 total, 2 done (100.00%)
    0:00 [3 rows, 39B] [12 rows/s, 159B/s]