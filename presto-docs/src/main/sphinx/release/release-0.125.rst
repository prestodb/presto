=============
Release 0.125
=============

General Changes
---------------

* Fix an issue where certain operations such as ``GROUP BY``, ``DISTINCT``, etc. on the
  output of a ``RIGHT`` or ``FULL OUTER JOIN`` can return incorrect results if they reference columns
  from the left relation that are also used in the join clause, and not every row from the right relation
  has a match.
