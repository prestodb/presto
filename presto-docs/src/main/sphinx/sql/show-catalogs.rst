=============
SHOW CATALOGS
=============

Synopsis
--------

.. code-block:: none

    SHOW CATALOGS [ LIKE pattern ]

Description
-----------

List the available catalogs.

:ref:`Specify a pattern <like_operator>` in the optional ``LIKE`` clause to
filter the results to the desired subset. For example, the following query
allows you to find catalogs that begin with ``t``::

    SHOW CATALOGS LIKE 't%';
