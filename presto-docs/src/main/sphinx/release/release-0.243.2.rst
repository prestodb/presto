===============
Release 0.243.2
===============

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured (:pr:`15501`).

General Changes
---------------
* Fix LambdaDefinitionExpression canonicalization to correctly canonicalize Block constant (:issue:`15424`).
