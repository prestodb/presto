=============
Release 0.140
=============

General Changes
---------------

* Add the ``TRY`` function to handle specific data exceptions. See
  :doc:`/functions/conditional`.
* Optimize predicate expressions to minimize redundancies.
* Add environment name to UI.
* Fix logging of ``failure_host`` and ``failure_task`` fields in
  ``QueryCompletionEvent``.
* Fix race which can cause queries to fail with a ``REMOTE_TASK_ERROR``.
* Optimize :func:`array_distinct` for ``array<bigint>``.
* Optimize ``>`` operator for :ref:`array_type`.
* Fix an optimization issue that could result in non-deterministic functions
  being evaluated more than once producing unexpected results.
* Fix incorrect result for rare ``IN`` lists that contain certain combinations
  of non-constant expressions that are null and non-null.
* Improve performance of joins, aggregations, etc. by removing unnecessarily
  duplicated columns.
* Optimize ``NOT IN`` queries to produce more compact predicates.

Hive Changes
------------

* Remove bogus "from deserializer" column comments.
* Change categorization of Hive writer errors to be more specific.
* Add date and timestamp support to new Parquet Reader

SPI Changes
-----------

* Remove partition key from ``ColumnMetadata``.
* Change return type of ``ConnectorTableLayout.getDiscretePredicates()``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
