=============
Release 0.122
=============

.. warning::

   There is a bug in this release that will cause queries to fail when the
   ``optimizer.optimize-hash-generation`` config is disabled.

General Changes
---------------

* The deprecated casts between JSON and VARCHAR will now fail and provide the
  user with instructions to migrate their query. For more details, see
  :doc:`/release/release-0.116`.
* Fix ``NoSuchElementException`` when cross join is used inside ``IN`` query.
* Fix ``GROUP BY`` to support maps of structural types.
* The web interface now displays a lock icon next to authenticated users.
* The :func:`min_by` and :func:`max_by` aggregations now have an additional form
  that return multiple values.
* Fix incorrect results when using ``IN`` lists of more than 1000 elements of
  ``timestamp with time zone``, ``time with time zone`` or structural types.
