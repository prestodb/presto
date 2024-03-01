=============
Release 0.228
=============

General Changes
_______________
* Reduce excessive memory usage by ExchangeClient.
* Improve coordinator stability on parsing functions that may create large constant
  arrays/maps/rows (e.g., ``SEQUENCE``, ``REPEAT``, ``ARRAY[...]``, etc) by delaying the
  evaluation on these functions on workers.
* Optimize queries with ``LIMIT 0``.
* Allow Bing Tiles at zoom level 0.

Hive Changes
____________
* Fix ORC writer rollback failure due to exception thrown during rollback.
* Improve ORC read performance for variants and fixed width numbers.

Spi Changes
___________
* Move most ``RowExpression`` utilities to the ``presto-expressions`` module.
