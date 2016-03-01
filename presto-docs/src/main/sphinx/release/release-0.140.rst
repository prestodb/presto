=============
Release 0.140
=============

General Changes
---------------

* Optimize predicate expressions to minimize redundancies.
* Add environment name to UI.
* Fix logging of ``failure_host`` and ``failure_task`` fields in
  ``QueryCompletionEvent``.
* Fix race which can cause queries to fail with a ``REMOTE_TASK_ERROR``.
* Optimize :func:`array_distinct` for ``array<bigint>``.
* Optimize ``>`` operator for :ref:`array_type`.

Hive Changes
------------

* Remove bogus "from deserializer" column comments.

SPI Changes
-----------

* Remove partition key from ``ColumnMetadata``.
* Change return type of ``ConnectorTableLayout.getDiscretePredicates()``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
