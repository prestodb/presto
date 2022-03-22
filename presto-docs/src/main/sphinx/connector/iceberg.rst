=================
Iceberg Connector
=================

Overview
--------

The Iceberg connector allows querying data stored in Iceberg tables.

.. note::

    It is recommended to use Iceberg 0.9.0 or later.

Configuration
-------------

To configure the Iceberg connector, create a catalog properties file
``etc/catalog/iceberg.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=iceberg
    hive.metastore.uri=hostname:port

Configuration Properties
------------------------

The following configuration properties are available:

======================= ==================================
Property Name           Description
======================= ==================================
``hive.metastore.uri``  The URI(s) of the Hive metastore.
======================= ==================================

``hive.metastore.uri``
^^^^^^^^^^^^^^^^^^^^^^
The URI(s) of the Hive metastore to connect to using the Thrift protocol.
If multiple URIs are provided, the first URI is used by default and the
rest of the URIs are fallback metastores. This property is required.
Example: ``thrift://192.0.2.3:9083`` or ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

Iceberg Configuration Properties
--------------------------------

============================== ================================================= ============
Property Name                  Description                                       Default
============================== ================================================= ============
``iceberg.file-format``        The storage file format for Iceberg tables.       ``PARQUET``

``iceberg.compression-codec``  The compression codec to use when writing files.  ``GZIP``
============================== ================================================= ============
