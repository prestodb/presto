==================
Presto C++ Sidecar
==================

In a Presto C++ cluster, the coordinator communicates with the Presto C++ sidecar
process to better support Presto C++ specific functionality. This chapter documents
the REST API used in these communications and the configuration properties
pertaining to the Presto C++ sidecar.

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Endpoints
---------

The following HTTP methods are used by the Presto coordinator to fetch data from
the Presto C++ sidecar.

* A ``GET`` on ``/v1/functions`` returns JSON containing the function metadata for
  all functions registered in the sidecar. The JSON has the function names as keys,
  and a JSON array of function metadata, each conforming to the
  ``protocol::JsonBasedUdfFunctionMetadata`` format.


Properties
----------

Set the following configuration properties for the Presto C++ sidecar.

.. code-block:: none

    native-sidecar=true

