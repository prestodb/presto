===========
SHOW TABLES
===========

Synopsis
--------

.. code-block:: none

    SHOW TABLES [ FROM schema ] [ LIKE pattern [ ESCAPE 'escape_character' ] ]

Description
-----------

List the tables in ``schema`` or in the current schema.

:ref:`Specify a pattern <like_operator>` in the optional ``LIKE`` clause to
filter the results to the desired subset.. For example, the following query
allows you to find tables that begin with ``p``::

    SHOW TABLES FROM tpch.tiny LIKE 'p%';
