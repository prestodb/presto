=============
Release 0.151
=============

General Changes
---------------

* Fix issue where aggregations may produce the wrong result when ``task.concurrency`` is set to ``1``.
* Fix query failure when ``array``, ``map``, or ``row`` type is used in non-equi ``JOIN``.
* Fix performance regression for queries using ``OUTER JOIN``.
* Fix query failure when using the :func:`arbitrary` aggregation function on ``integer`` type.
* Add various math functions that operate directly on ``float`` type.
* Add flag ``deprecated.legacy-array-agg`` to restore legacy :func:`array_agg`
  behavior (ignore ``NULL`` input). This flag will be removed in a future release.
* Add support for uncorrelated ``EXISTS`` clause.
* Add :func:`cosine_similarity` function.
* Allow :doc:`/installation/tableau` to use catalogs other than ``hive``.

Verifier Changes
----------------

* Add ``shadow-writes.enabled`` option which can be used to transform ``CREATE TABLE AS SELECT``
  queries to write to a temporary table (rather than the originally specified table).

SPI Changes
-----------

* Remove ``getDataSourceName`` from ``ConnectorSplitSource``.
* Remove ``dataSourceName`` constructor parameter from ``FixedSplitSource``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
