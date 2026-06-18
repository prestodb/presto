===============
Release 0.233.1
===============

Hive Changes
____________
* Fix an issue where queries with the predicate ``IS NULL`` on bucketed columns would produce
  incorrect results. (:pr:`14276`).
