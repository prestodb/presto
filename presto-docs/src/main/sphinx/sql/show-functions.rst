==============
SHOW FUNCTIONS
==============

Synopsis
--------

.. code-block:: none

    SHOW FUNCTIONS [ LIKE pattern [ ESCAPE 'escape_character' ] ]

Description
-----------

List all functions available for use in queries.

:ref:`Specify a pattern <like_operator>` in the optional ``LIKE`` clause to
filter the results to the desired subset. For example, the following query
allows you to find functions beginning with ``array``::

    SHOW FUNCTIONS LIKE 'array%';
