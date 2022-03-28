========
TRUNCATE
========

Synopsis
--------

.. code-block:: none

    TRUNCATE TABLE table_name

Description
-----------

Delete all rows from a table.

Examples
--------

Truncate the table ``orders``::

    TRUNCATE TABLE orders;

Limitations
-----------

Some connectors have limited or no support for ``TRUNCATE``.
See connector documentation for more details.
