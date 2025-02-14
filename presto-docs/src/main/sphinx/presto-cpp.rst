**********
Presto C++
**********

Note: Presto C++ is in active development. See :doc:`Limitations </presto_cpp/limitations>`.

.. toctree::
    :maxdepth: 1

    presto_cpp/features
    presto_cpp/limitations
    presto_cpp/plugin
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

* Hive connector for reads and writes, including CTAS, are supported.

* Hive Connector supports Delta Lake table access using Symlink table in Prestissimo.
  For more information about Symlink tables, see `Presto, Trino, and Athena to Delta Lake integration using
  manifests <https://docs.delta.io/latest/presto-integration.html>`_.

* Iceberg tables are supported only for reads.

* Iceberg connector supports both V1 and V2 tables, including tables with delete files.

* TPCH connector, with ``tpch.naming=standard`` catalog property.