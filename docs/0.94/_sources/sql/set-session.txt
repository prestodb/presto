===========
SET SESSION
===========

Synopsis
--------

.. code-block:: none

    SET SESSION name = 'value'
    SET SESSION catalog.name = 'value'

Description
-----------

Set a session property value.

Examples
--------

.. code-block:: sql

    SET SESSION optimize_hash_generation = 'true';
    SET SESSION hive.optimized_reader_enabled = 'true';
