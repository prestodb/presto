===============
Release 0.243.4
===============

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured (:pr:`15501`).

Hive Changes
____________
* Fix reading ORC files having MAP columns with MAP_FLAT encoding where all entries are empty maps (:pr:`15468`).
