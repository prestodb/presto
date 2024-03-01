=============
Release 0.246
=============

.. warning::
    There is a bug causing ``SORT`` or``LIMIT`` to be incorrectly eliminated when using ``GROUPING SETS (())`, ``CUBE`` or ``ROLLUP``,
    first introduced in 0.246 by :pr:`14915`

**Details**
===========

General Changes
_______________
* Fix a bug introduced in :pr:`15313` that would cause queries to fail when using upper case in SQL function catalog schema names.
* Fix a possible integer overflow error when spilling to temporary storage.
* Fix an issue where prepared statements would allow some non-constant parameters.
* Fix an error where Presto server can fail to start when using function namespace manager.
* Add a minimum value of 30 seconds to the configuration property ``query.min-expire-age``.
* Add listener-based revocation model for spilling strategy ``PER_TASK_MEMORY_THRESHOLD``.
* Disable spill to disk for join queries where the probe side uses grouped execution and the build does not. Previously these queries would fail with ``GENERIC_INTERNAL_ERRORS``.
* Disallow ``ORDER BY`` literals used with Window functions as it's not useful, expensive and most often used wrongly.
* Add support for fragment result caching for queries with local exchange (e.g. intermediate aggregation).
* Improve queries that have unnecessary limits and order bys.
  This feature is enabled by default and can be disabled by using the configuration property ``optimizer.skip-redundant-sort`` or session property ``skip_redundant_sort``.

Geospatial Changes
__________________
* Upgrade JTS to 1.18.0.

Hive Changes
____________
* Fix dynamic pruning failures for joining on null keys in hive partition.

**Contributors**
================

Andrii Rosa, Ariel Weisberg, Bhavani Hari, Bin Fan, Emy Sun, George Wang, James Gill, James Petty, James Sun, John Roll, Maria Basmanova, Moji Solgi, Nikhil Collooru, Rebecca Schlussel, Rohit Jain, Rongrong Zhong, Saksham Sachdev, Shixuan Fan, Sorin Stoiana, Sreeni Viswanadha, Vic Zhang, Wenlei Xie, Ying Su, Zhenyuan Zhao, fornaix
