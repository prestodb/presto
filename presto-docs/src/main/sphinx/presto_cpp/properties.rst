===============================
Presto C++ Properties Reference
===============================

This section describes Presto C++ configuration properties.

The following is not a complete list of all configuration and
session properties, and does not include any connector-specific
catalog configuration properties. For information on catalog 
configuration properties, see :doc:`Connectors </connector/>`.

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Coordinator Properties
----------------------

Set the following configuration properties for the Presto coordinator exactly 
as they are shown in this code block to enable the Presto coordinator's use of 
Presto C++ workers. 

.. code-block:: none

    native-execution-enabled=true
    optimizer.optimize-hash-generation=false
    regex-library=RE2J
    use-alternative-function-signatures=true
    experimental.table-writer-merge-operator-enabled=false

These Presto coordinator configuration properties are described here, in 
alphabetical order. 

``experimental.table-writer-merge-operator-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

  Merge TableWriter output before sending to TableFinishOperator. This property must be set to 
  ``false``. 

``native-execution-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

  This property is required when running Presto C++ workers because of 
  underlying differences in behavior from Java workers.

``optimizer.optimize-hash-generation``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

  Set this property to ``false`` when running Presto C++ workers.  
  Velox does not support optimized hash generation, instead using a HashTable 
  with adaptive runtime optimizations that does not use extra hash fields. 

``regex-library``
^^^^^^^^^^^^^^^^^

* **Type:** ``type``
* **Allowed values:** ``RE2J``
* **Default value:** ``JONI``

  Only `RE2J <https://github.com/google/re2j>`_ is currently supported by Velox.

``use-alternative-function-signatures``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``false``

  Some aggregation functions use generic intermediate types which are 
  not compatible with Velox aggregation function intermediate types. One  
  example function is ``approx_distinct``, whose intermediate type is 
  ``VARBINARY``. 
  This property provides function signatures for built-in aggregation 
  functions which are compatible with Velox.

Worker Properties
-----------------

The configuration properties of Presto C++ workers are described here, in alphabetical order. 

``async-data-cache-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

  In-memory cache.

``query.max-memory-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``4GB``

  Max memory usage for each query.

``query-memory-gb``
^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``38``

  The total memory capacity that can be used across all query executions. 
  Memory for system usage such as disk spilling and cache prefetch which 
  are not counted in query memory usage.

``query-reserved-memory-gb``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``4``

  Specifies the amount of query memory capacity reserved 
  to ensure that each query has the minimal memory capacity to run. A query can 
  only allocate from the reserved query memory if its current capacity is less 
  than the minimal memory capacity as specified by 
  ``memory-pool-reserved-capacity``. 
  
  The exceeding capacity must allocate from the non-reserved query memory.

``system-memory-gb``
^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``40``

  Memory allocation limit enforced via internal memory allocator. 

  Set ``system-memory-gb`` to the available machine memory of the deployment.

``task.max-drivers-per-task``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``integer``
* **Default value:** ``number of hardware CPUs``

  Number of drivers to use per task. Defaults to hardware CPUs.

