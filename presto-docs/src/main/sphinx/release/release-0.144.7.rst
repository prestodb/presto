===============
Release 0.144.7
===============

General Changes
---------------

* Fail queries with non-equi conjuncts in ``OUTER JOIN``\s, instead of silently
  dropping such conjuncts from the query and producing incorrect results.
* Add :func:`cosine_similarity` function.
