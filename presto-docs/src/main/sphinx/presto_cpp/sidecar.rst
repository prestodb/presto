===================
Presto C++ Sidecar
===================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Endpoints
---------

The following HTTP endpoints are implemented by the Presto C++ sidecar:

* GET: v1/properties/session: Returns session properties supported by the
  Presto C++ worker serialized as JSON.
* GET: v1/functions: Returns metadata for functions supported by the Presto
  C++ worker, serialized as JSON.
* POST: v1/velox/plan: Converts a Presto plan fragment to it's corresponding
  Velox plan, returns any errors encountered during plan conversion.

Sidecar Properties
------------------

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

The prefix used to register all Presto C++ functions. The registered
function names are of type ``catalog.schema.function_name``. For Presto
C++ functions, the default ``catalog.schema`` is ``native.default``.

Troubleshooting
---------------

Some common problems with the Presto C++ sidecar and how to troubleshoot: