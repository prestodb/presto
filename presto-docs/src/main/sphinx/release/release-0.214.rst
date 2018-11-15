=============
Release 0.214
=============

General Changes
---------------

* Fix history leak in coordinator for failed or canceled queries.
* Fix memory leak related to query tracking in coordinator that was introduced
  in :doc:`/release/release-0.213`.
* Fix planning failures when lambdas are used in join filter expression.
* Fix responses to client for certain types of errors that are encountered
  during query creation.
* Improve error message when an invalid comparator is provided to the
  :func:`array_sort` function.
* Improve performance of lookup operations on map data types.
* Improve planning and query performance for queries with ``TINYINT``,
  ``SMALLINT`` and ``VARBINARY`` literals.
* Fix issue where queries containing distributed ``ORDER BY`` and aggregation
  could sometimes fail to make progress when data was spilled.
* Make top N row number optimization work in some cases when columns are pruned.
* Add session property ``optimize-top-n-row-number`` and configuration property
  ``optimizer.optimize-top-n-row-number`` to toggle the top N row number
  optimization.
* Add :func:`ngrams` function to generate N-grams from an array.
* Add :ref:`qdigest <qdigest_type>` type and associated :doc:`/functions/qdigest`.
* Add functionality to delay query execution until a minimum number of workers
  nodes are available. The minimum number of workers can be set with the
  ``query-manager.required-workers`` configuration property, and the max wait
  time with the ``query-manager.required-workers-max-wait`` configuration property.
* Remove experimental pre-allocated memory system, and the related configuration
  property ``experimental.preallocate-memory-threshold``.

Security Changes
----------------

* Add functionality to refresh the configuration of file-based access controllers.
  The refresh interval can be set using the ``security.refresh-period``
  configuration property.

JDBC Driver Changes
-------------------

* Clear update count after calling ``Statement.getMoreResults()``.

Web UI Changes
--------------

* Show query warnings on the query detail page.
* Allow selecting non-default sort orders in query list view.

Hive Connector Changes
----------------------

* Prevent ORC writer from writing stripes larger than the maximum configured size.
* Add ``hive.s3.upload-acl-type`` configuration property to specify the type of
  ACL to use while uploading files to S3.
* Add Hive metastore API recording tool for remote debugging purposes.
* Add support for retrying on metastore connection errors.

Verifier Changes
----------------

* Handle SQL execution timeouts while rewriting queries.
