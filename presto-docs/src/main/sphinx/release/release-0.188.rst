=============
Release 0.188
=============

General Changes
---------------

* Fix handling of negative start indexes in array :func:`slice` function.
* Fix inverted sign for time zones ``Etc/GMT-12``, ``Etc/GMT-11``, ..., ``Etc/GMT-1``,
  ``Etc/GMT+1``, ... ``Etc/GMT+12``.
* Improve performance of server logging and HTTP request logging.
* Reduce GC spikes by compacting join memory over time instead of all at once
  when memory is low. This can increase reliability at the cost of additional
  CPU. This can be enabled via the ``pages-index.eager-compaction-enabled``
  config property.
* Improve performance of and reduce GC overhead for compaction of in-memory data structures,
  primarily used in joins.
* Mitigate excessive GC and degraded query performance by forcing expiration of
  generated classes for functions and expressions one hour after generation.
* Mitigate performance issue caused by JVM when generated code is used
  for multiple hours or days.

CLI Changes
-----------

* Fix transaction support. Previously, after the first statement in the
  transaction, the transaction would be abandoned and the session would
  silently revert to auto-commit mode.

JDBC Driver Changes
-------------------

* Support using ``Statement.cancel()`` for all types of statements.

Resource Group Changes
----------------------

* Add environment support to the ``db`` resource groups manager.
  Previously, configurations for different clusters had to be stored in separate databases.
  With this change, different cluster configurations can be stored in the same table and
  Presto will use the new ``environment`` column to differentiate them.

SPI Changes
-----------

* Add query plan to the query completed event.
