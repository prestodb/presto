===============
Release 0.243.1
===============

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured (:pr:`15501`).

Hive Changes
------------
* Fix a bug with reading encrypted DWRF tables where queries could fail with a NullPointerException.
