===============
Release 0.245.1
===============

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured (:pr:`15501`).

Hive Changes
____________
* Fix a bug reading ORC files with ``ARRAY``/``MAP``/``ROW`` of ``VARCHAR`` columns using the selective stream readers for some corner cases (:pr:`15549`).