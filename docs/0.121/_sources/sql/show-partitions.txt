===============
SHOW PARTITIONS
===============

Synopsis
--------

.. code-block:: none

    SHOW PARTITIONS FROM table [ WHERE ... ] [ ORDER BY ... ] [ LIMIT ... ]

Description
-----------

List the partitions in ``table``, optionally filtered using the ``WHERE`` clause,
ordered using the ``ORDER BY`` clause and limited using the ``LIMIT`` clause.
These clauses work the same way that they do in a :doc:`select` statement.

Examples
--------

List all partitions in the table ``orders``::

    SHOW PARTITIONS FROM orders;

List all partitions in the table ``orders`` starting from the year ``2013``
and sort them in reverse date order::

    SHOW PARTITIONS FROM orders WHERE ds >= '2013-01-01' ORDER BY ds DESC;

List the most recent partitions in the table ``orders``::

    SHOW PARTITIONS FROM orders ORDER BY ds DESC LIMIT 10;
