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

.. function:: POST /v1/velox/plan

   Converts a Presto plan fragment to its corresponding Velox plan and
   validates the Velox plan. Returns any errors encountered during plan
   conversion.

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
