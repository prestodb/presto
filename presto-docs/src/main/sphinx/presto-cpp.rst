**********
Presto C++
**********

Note: Presto C++ is in active development. See :doc:`Limitations </presto_cpp/limitations>`.

.. toctree::
    :maxdepth: 1

    presto_cpp/features
    presto_cpp/sidecar
    presto_cpp/limitations
    presto_cpp/properties
    presto_cpp/properties-session

Overview
========

Presto C++, sometimes referred to by the development name Prestissimo, is a 
drop-in replacement for Presto workers written in C++ and based on the 
`Velox <https://velox-lib.io/>`_ library. 
It implements the same RESTful endpoints as Java workers using the Proxygen C++ 
HTTP framework.
Because communication with the Java coordinator and across workers is only 
done using the REST endpoints, Presto C++ does not use JNI and does not 
require a JVM on worker nodes.

A Presto C++ worker can be configured as a sidecar to customize the Java
coordinator's functionality for Presto C++ clusters. The Presto C++ sidecar
implements additional REST endpoints to provide the coordinator more
information about the Presto C++ worker, such as session properties and
functions, via the ``NativeSidecarPlugin``. It is recommended to configure
at least one worker in the Presto C++ cluster as a sidecar. See :doc:`presto_cpp/sidecar`
and :ref:`native-sidecar-plugin` for more details.

Presto C++'s codebase is located at `presto-native-execution
<https://github.com/prestodb/presto/tree/master/presto-native-execution>`_.

Motivation and Vision
=====================

Presto aims to be the top performing system for data lakes. 
To achieve this goal, the Presto community is moving the Presto 
evaluation engine from the native Java-based implementation to a new 
implementation written in C++ using `Velox <https://velox-lib.io/>`_.  

By moving the evaluation engine to a library, the intent is to enable the 
Presto community to focus on more features and better integration with table 
formats and other data warehousing systems.

Supported Use Cases
===================

Only specific connectors are supported in the Presto C++ evaluation engine.

Hive Connector
^^^^^^^^^^^^^^

Hive Table Support
~~~~~~~~~~~~~~~~~~

* Hive connector for reads and writes, including CTAS, are supported.

* Hive connector supports Delta Lake table access using Symlink table in Presto C++.
  For more information about Symlink tables, see `Presto, Trino, and Athena to Delta Lake integration using
  manifests <https://docs.delta.io/latest/presto-integration.html>`_.

* Supports reading and writing of DWRF and PARQUET file formats.

* Supports reading ORC file format.

Iceberg Table Support
~~~~~~~~~~~~~~~~~~~~~

* Iceberg tables use the Hive connector implementation in Velox.

* Only read operations are supported for Iceberg tables.

* The Iceberg connector supports both V1 and V2 tables, including those with positional delete files, but does not support equality delete files.

For more information see :doc:`/connector/iceberg` documentation.

TPCH Connector
^^^^^^^^^^^^^^

* TPCH connector, with ``tpch.naming=standard`` catalog property.