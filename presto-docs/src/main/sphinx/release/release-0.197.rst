=============
Release 0.197
=============

General Changes
---------------

* Fix query scheduling hang when the ``concurrent_lifespans_per_task`` session property is set.
* Fix failure when a query contains a ``TIMESTAMP`` literal corresponding to a local time that
  does not occur in the default time zone of the Presto JVM. For example, if Presto was running
  in a CET zone (e.g., ``Europe/Brussels``) and the client session was in UTC, an expression
  such as ``TIMESTAMP '2017-03-26 02:10:00'`` would cause a failure.
* Extend predicate inference and pushdown for queries using a ``<symbol> IN <subquery>`` predicate.
* Support predicate pushdown for the ``<column> IN <values list>`` predicate
  where values in the ``values list`` require casting to match the type of ``column``.
* Optimize :func:`min` and :func:`max` to avoid unnecessary object creation in order to reduce GC overhead.
* Optimize the performance of :func:`ST_XMin`, :func:`ST_XMax`, :func:`ST_YMin`, and :func:`ST_YMax`.
* Add ``DATE`` variant for :func:`sequence` function.
* Add :func:`ST_IsSimple` geospatial function.
* Add support for broadcast spatial joins.

Resource Groups Changes
-----------------------

* Change configuration check for weights in resource group policy to validate that
  either all of the subgroups or none of the subgroups have a scheduling weight configured.
* Add support for named variables in source and user regular expressions that can be
  used to parameterize resource group names.
* Add support for optional fields in DB resource group exact match selectors.

Hive Changes
------------

* Fix reading of Hive partition statistics with unset fields. Previously, unset fields
  were incorrectly interpreted as having a value of zero.
* Fix integer overflow when writing a single file greater than 2GB with optimized ORC writer.
* Fix system memory accounting to include stripe statistics size and
  writer validation size for the optimized ORC writer.
* Dynamically allocate the compression buffer for the optimized ORC writer
  to avoid unnecessary memory allocation. Add config property
  ``hive.orc.writer.max-compression-buffer-size`` to limit the maximum size of the buffer.
* Add session property ``orc_optimized_writer_max_stripe_size`` to tune the
  maximum stipe size for the optimized ORC writer.
* Add session property ``orc_string_statistics_limit`` to drop the string
  statistics when writing ORC files if they exceed the limit.
* Use the view owner returned from the metastore at the time of the query rather than
  always using the user who created the view. This allows changing the owner of a view.

CLI Changes
-----------

* Fix hang when CLI fails to communicate with Presto server.

SPI Changes
-----------

* Include connector session properties for the connector metadata calls made
  when running ``SHOW`` statements or querying ``information_schema``.
* Add count and time of full GC that occurred while query was running to ``QueryCompletedEvent``.
* Change the ``ResourceGroupManager`` interface to include a ``match()`` method and
  remove the ``getSelectors()`` method and the ``ResourceGroupSelector`` interface.
* Rename the existing ``SelectionContext`` class to be ``SelectionCriteria`` and
  create a new ``SelectionContext`` class that is returned from the ``match()`` method
  and contains the resource group ID and a manager-defined context field.
* Use the view owner from ``ConnectorViewDefinition`` when present.
