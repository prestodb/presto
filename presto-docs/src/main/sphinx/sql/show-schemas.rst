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

    Name of the catalog

Examples
--------

.. code-block:: none

    presto:default> show schemas;
           Schema       
    --------------------
     information_schema 
     jmx                
     sys                
    (3 rows)
