=============
Release 0.111
=============

General Changes
---------------

* Add :func:`histogram` function.
* Optimize ``CASE`` expressions on a constant.
* Add basic support for ``IF NOT EXISTS`` for ``CREATE TABLE``.
* Semi-joins are hash-partitioned if ``distributed_join`` is turned on.
* Add support for partial cast from JSON. For example, ``json`` can be cast to ``array(json)``, ``map(varchar, json)``, etc.
* Add implicit coercions for ``UNION``.
* Expose query stats in the JDBC driver ``ResultSet``.
