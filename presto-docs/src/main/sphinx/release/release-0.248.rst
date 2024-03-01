=============
Release 0.248
=============

.. warning::
    There is a bug causing ``SORT`` or``LIMIT`` to be incorrectly eliminated when using ``GROUPING SETS (())`, ``CUBE`` or ``ROLLUP``,
    first introduced in 0.246 by :pr:`14915`

**Highlights**
==============
* New aggregation function :func:`map_union_sum`.
* Add support for overriding session properties using session property managers. See :doc:`/admin/session-property-managers`.

**Details**
===========

General Changes
_______________
* Improve query performance by reducing lock contention in output buffer memory tracking.
* Add support for overriding session properties using session property managers :doc:`/admin/session-property-managers`. Setting ``overrideSessionProperties`` to true will cause the property to be overridden and remain overridden even if subsequent rules match the property but don't have ``overrideSessionProperties`` set.
* Add support to drop multiple UDFs at the same time.
* Add a REST endpoint ``/v1/taskInfo/{{taskId}}`` on the coordinator to get TaskInfo without needing to go directly to the worker's endpoint.
* Add new aggregation function :func:`map_union_sum`.
* Add support to configure ZSTD compression level for ORC writer.
* Add warning for JOIN conditions with OR expressions.
* Add configuration property ``internal-communication.https.trust-store-password`` to set the Java Truststore password used for https in internal communications between nodes.

Hive Connector Changes
________________
* Add session property ``temporary_table_create_empty_bucket_files`` and configuration property ``hive.create-empty-bucket-files-for-temporary-table``, which, when set to ``false``, disables the creation of zero-row files for temporary table empty buckets, to improve performance.

Verifier Changes
________________
* Add output table names to Presto Verifier outputs.

**Contributors**
================

Ajay George, Andrii Rosa, Ariel Weisberg, Arunachalam Thirupathi, Bin Fan, Chi Tsai, David Stryker, James Petty, James Sun, Ke Wang, Leiqing Cai, Luca, Lung-Yen Chen, Nikhil Collooru, Rebecca Schlussel, Rongrong Zhong, Shixuan Fan, Sreeni Viswanadha, Stephen Dimmick, Tim Meehan, Venki Korukanti, Vic Zhang, Wenlei Xie, Yang Yang
