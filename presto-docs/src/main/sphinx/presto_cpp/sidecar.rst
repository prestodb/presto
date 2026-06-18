===================
Presto C++ Sidecar
===================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Endpoints
---------

The following HTTP endpoints are implemented by the Presto C++ sidecar.

.. function:: GET /v1/properties/session

   Returns a list of system session properties supported by the Presto C++
   worker. Each session property is serialized to JSON in format
   ``SessionPropertyMetadata``.

.. function:: GET /v1/functions

   Returns a list of function metadata for all functions registered in the
   Presto C++ worker. Each function's metadata is serialized to JSON in
   format ``JsonBasedUdfFunctionMetadata``.

.. function:: GET /v1/functions/{catalog}

   Returns a list of function metadata for all functions registered in the
   Presto C++ worker that belong to the specified catalog. Each function's
   metadata is serialized to JSON in format ``JsonBasedUdfFunctionMetadata``.
   This endpoint allows filtering functions by catalog to support namespace
   separation.

.. function:: POST /v1/velox/plan

   Converts a Presto plan fragment to its corresponding Velox plan and
   validates the Velox plan. Returns any errors encountered during plan
   conversion.

.. function:: POST /v1/expressions

   Optimizes a list of ``RowExpression``\s from the http request using
   a combination of constant folding and logical rewrites by leveraging
   the ``ExprOptimizer`` from Velox. Returns a list of ``RowExpressionOptimizationResult``,
   that contains either the optimized ``RowExpression`` or the ``NativeSidecarFailureInfo``
   in case the expression optimization failed.

Configuration Properties
------------------------

The following properties should be set on the Presto C++ sidecar:

``native-sidecar``
^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

Set this to ``true`` to configure the Presto C++ worker as a sidecar.

``presto.default-namespace``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** ``"native.default"``

All functions registered in Presto are named in format ``catalog.schema.function_name``,
this property defines the prefix used to register all Presto C++ functions.
The default namespace should be of type ``catalog.schema`` and it is
recommended to set it to ``native.default``.
