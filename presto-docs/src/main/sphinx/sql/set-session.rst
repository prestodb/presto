===========
SET SESSION
===========

Synopsis
--------

.. code-block:: none

    SET SESSION name = expression
    SET SESSION catalog.name = expression

Description
-----------

Set a session property value.

Examples
--------

.. code-block:: sql

    SET SESSION optimize_hash_generation = true;
    SET SESSION hive.optimized_reader_enabled = true;

See Also
--------

:doc:`reset-session`, :doc:`show-session`
