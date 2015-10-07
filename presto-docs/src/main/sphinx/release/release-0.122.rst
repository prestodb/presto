=============
Release 0.122
=============

General Changes
---------------

* The deprecated casts between JSON and VARCHAR will now fail and provide the
  user with instructions to migrate their query. For more details, see
  :doc:`/release/release-0.116`.
* * Fix ``NoSuchElementException`` when cross join is used inside ``IN`` query.
