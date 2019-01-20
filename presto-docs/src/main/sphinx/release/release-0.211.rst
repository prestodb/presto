=============
Release 0.211
=============

General Changes
---------------

* Fix missing final query plan in ``QueryCompletedEvent``. Statistics and cost estimates
  are removed from the plan text because they may not be available during event generation.
* Update the default value of the ``http-server.https.excluded-cipher`` config
  property to exclude cipher suites with a weak hash algorithm or without forward secrecy.
  Specifically, this means all ciphers that use the RSA key exchange are excluded by default.
  Consequently, TLS 1.0 or TLS 1.1 are no longer supported with the default configuration.
  The ``http-server.https.excluded-cipher`` config property can be set to empty string
  to restore the old behavior.
* Add :func:`ST_GeomFromBinary` and :func:`ST_AsBinary` functions that convert
  geometries to and from Well-Known Binary format.
* Remove the ``verbose_stats`` session property, and rename the ``task.verbose-stats``
  configuration property to ``task.per-operator-cpu-timer-enabled``.
* Improve query planning performance for queries containing multiple joins
  and a large number of columns (:issue:`x11196`).
* Add built-in :doc:`file based property manager </admin/session-property-managers>`
  to automate the setting of session properties based on query characteristics.
* Allow running on a JVM from any vendor that meets the functional requirements.

Hive Connector Changes
----------------------

* Fix regression in 0.210 that causes query failure when writing ORC or DWRF files
  that occurs for specific patterns of input data. When the writer attempts to give up
  using dictionary encoding for a column that is highly compressed, the process of
  transitioning to use direct encoding instead can fail.
* Fix coordinator OOM when a query scans many partitions of a Hive table (:issue:`x11322`).
* Improve readability of columns, partitioning, and transactions in explain plains.

Thrift Connector Changes
------------------------

* Fix lack of retry for network errors while sending requests.

Resource Group Changes
----------------------

* Add documentation for new resource group scheduling policies.
* Remove running and queue time limits from resource group configuration.
  Legacy behavior can be replicated by using the
  :doc:`file based property manager </admin/session-property-managers>`
  to set session properties.

SPI Changes
-----------

* Clarify semantics of ``predicate`` in ``ConnectorTableLayout``.
* Reduce flexibility of ``unenforcedConstraint`` that a connector can return in ``getTableLayouts``.
  For each column in the predicate, the connector must enforce the entire domain or none.
* Make the null vector in ``ArrayBlock``, ``MapBlock``, and ``RowBlock`` optional.
  When it is not present, all entries in the ``Block`` are non-null.
