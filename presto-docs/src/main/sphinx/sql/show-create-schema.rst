==================
SHOW CREATE SCHEMA
==================

Synopsis
--------

.. code-block:: none

    SHOW CREATE SCHEMA schema_name

Description
-----------

Show the SQL statement that creates the specified schema.

Examples
--------

Show the SQL that can be run to create the ``sf1`` schema::

    SHOW CREATE SCHEMA hive.sf1;

.. code-block:: none

                  Create Schema
    -----------------------------------------
     CREATE SCHEMA hive.sf1
     WITH (
        location = 'hdfs://localhost:9000/user/hive/warehouse/sf1.db'
     )
    (1 row)

See Also
--------

:doc:`create-schema`
