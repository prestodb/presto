=============
CREATE SCHEMA
=============

Synopsis
--------

.. code-block:: none

    CREATE SCHEMA [ IF NOT EXISTS ] schema_name
    [ WITH ( property_name = expression [, ...] ) ]

Description
-----------

Create a new, empty schema. A schema is a container that
holds tables, views and other database objects.

The optional ``IF NOT EXISTS`` clause causes the error to be
suppressed if the schema already exists.

The optional ``WITH`` clause can be used to set properties
on the newly created schema.  To list all available schema
properties, run the following query::

    SELECT * FROM system.metadata.schema_properties

Examples
--------

Create a new schema ``web`` in the current catalog::

    CREATE SCHEMA web

Create a new schema ``sales`` in the ``hive`` catalog::

    CREATE SCHEMA hive.sales

Create the schema ``traffic`` if it does not already exist::

    CREATE SCHEMA IF NOT EXISTS traffic

See Also
--------

:doc:`alter-schema`, :doc:`drop-schema`
