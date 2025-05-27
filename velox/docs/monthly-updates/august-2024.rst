******************
August 2024 Update
******************

This month has **144** commits from **45** authors. Below are some of the highlights.

Core Library
============

* Add IPAddress Presto type.
* Add support for copying a vector to a new memory pool. :pr:`10647`
* Add support for Wave metrics.
* Add support for byte input stream backed by a file. :pr:`10717`
* Add near zero-copy import & export for Arrow BinaryView/Utf8View formats. :pr:`9726`
* Add support for custom join bridges. :pr:`10626`
* Add support for row-level streaming build in the window operator. :pr:`9025`
* Fix some joins to preserve probe order. :pr:`10651`, :pr:`10832`
* Fix row vectors export to Arrow. :pr:`10703`
* Fix memory arbitration/reclaim in single-threaded execution. :pr:`10600`
* Fix filter evaluation for missing fields in a file. :pr:`10777`
* Fix a race condition in query memory abort process. :pr:`10826`
* Fix common sub-expression. :pr:`10837`

Presto Functions
================

* Add support for WEEK_OF_WEEK_YEAR to :func:`date_format` function.
* Add support for UNKNOWN type in :func:`map_concat` function.
* Add support for complex types in :func:`array_except`, :func:`array_intersect`,
  and :func:`arrays_overlap`.
* Fix `json_array_*` functions to return NULL for malformed inputs.
* Fix NaN comparison in :func:`approx_percentile`. :pr:`10710`
* Fix :func:`array_join` for TIMESTAMP inputs. :pr:`10881`

Spark Functions
===============

* Add :spark:func:`empty2null`, :spark:func:`slice` scalar functions.
* Add :spark:func:`mode` aggregate function.
* Add support for UNKNOWN type in :spark:func:`hash` function.
* Add support for regex delimiter and limit argument in :spark:func:`split` function.
* Fix :spark:func:`collect_set` output. :pr:`10737`

Hive Connector
==============

* Add support for column names to include space character.
* Fix Parquet RowGroup filtering for MAP and ARRAY types. :pr:`10510`
* Fix reading legacy array and map types in parquet. :pr:`9728`, :pr:`10753`

Performance and Correctness
===========================

* Add support for Spark query runner. :pr:`10357`
* Add Query Trace Writers and Readers. :pr:`10774`
* Optimize remaining filter to not eagerly materialize multi-referenced fields. :pr:`10645`
* Optimize cross joins for single record build side. :pr:`10726`

Build System
============

* Add support for Clang15.

Credits
=======
::

     3	Amit Dutta - Meta
     5	Bikramjeet Vig - Meta
     5	Chengcheng Jin - Intel
     3	Christian Zentgraf - IBM
     7	Daniel Hunte - Meta
     4	Deepak Majeti - IBM
     4	Hongze Zhang - Intel
     1	Huameng (Michael) Jiang - Meta
     1	Jacob Khaliqi - Meta
     4	Jacob Wujciak-Jens - Voltron Data
     6	Jia Ke - Intel
    14	Jialiang Tan
     8	Jimmy Lu - Meta
     2	Joe Abraham - IBM
     1	Karthikeyan Natarajan - Nvidia
     7	Ke Wang - Meta
     1	Kevin Wilfong - Meta
     5	Krishna Pai - Meta
     1	Masha Basmanova - Meta
     1	Muhammad Faisal - Meta
     1	Orri Erling - Meta
    11	Pedro Eugenio Rocha Pedreira - Meta
     2	Pramod Satya - IBM
     2	Satadru Pan - Meta
     1	Serge Druzkin - Meta
     1	Sergey Pershin - Meta
     1	Stan Ionascu - Meta
     1	Urvish Desai - ByteDance
     4	Wei He - Meta
     3	Yang Zhang - Alibaba Inc
     2	Zuyu ZHANG
     2	duanmeng - Tencent
     1	gaoyangxiaozhu
     1	hitarth
     1	kevincmchen
     1	lingbin
     1	mohsaka
     2	rexan
     6	Rui Mo - Intel
     1	snadukudy
    12	xiaoxmeng - Meta
     2	yan ma - Intel
     1	yingsu00
     1	zhaokuo
     1	Zhen Li - Microsoft
