===============
Release 0.243.3
===============

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured (:pr:`15501`).

General Changes
---------------
* Fix access control checks for columns in ``USING`` clause (:pr:`15333`).
