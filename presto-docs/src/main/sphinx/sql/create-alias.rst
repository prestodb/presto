============
CREATE ALIAS
============

Synopsis
--------

.. code-block:: none

    CREATE ALIAS alias_name FOR remote_name

Description
-----------

Defines an alias for a table. An established alias can be used in a
statement as a replacement for the remote_name specified in the create
statemetn.

Examples
--------

Create a new alis ``orders_by_date`` that summarizes ``orders_sorted_date``::

    CREATE ALIAS orders_by_date FOR orders_sorted_date
