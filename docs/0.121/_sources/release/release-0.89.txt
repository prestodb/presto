============
Release 0.89
============

DATE Type
---------
The memory representation of dates is now the number of days since January 1, 1970
using a 32-bit signed integer.

.. note::
    This is a backwards incompatible change with the previous date
    representation, so if you have written a connector, you will need to update
    your code before deploying this release.

General Changes
---------------

* ``USE CATALOG`` and ``USE SCHEMA`` have been replaced with :doc:`/sql/use`.
* Fix issue where ``SELECT NULL`` incorrectly returns 0 rows.
* Fix rare condition where ``JOIN`` queries could produce incorrect results.
* Fix issue where ``UNION`` queries involving complex types would fail during planning.
