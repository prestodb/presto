=============
RESET SESSION
=============

Synopsis
--------

.. code-block:: none

    RESET SESSION name
    RESET SESSION catalog.name

Description
-----------

Reset a session property value to the default value.

Examples
--------

.. code-block:: sql

    RESET SESSION optimize_hash_generation;
    RESET SESSION hive.optimized_reader_enabled;

See Also
--------

:doc:`set-session`, :doc:`show-session`
