====================
Tablestore Connector
====================

Overview
--------

The Tablestore Connector allows access to Alibaba Tablestore data from Presto.
This document describes how to setup the Tablestore Connector to run SQL queries against Alibaba Tablestore.

Configuration
-------------

To configure the Tablestore connector, create a catalog properties file
``etc/catalog/tablestore.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=tablestore
    tablestore.endpoint=https://demo.cn-hangzhou.ots.aliyuncs.com
    tablestore.access-key-id=the-real-key-id
    tablestore.access-key-secret=the-real-key-secret
    tablestore.instance=test

Configuration Properties
------------------------

The following configuration properties are available:

================================== ===================================================
Property Name                       Description
================================== ===================================================
``tablestore.endpoint``            The endpoint of tablestore.
``tablestore.access-key-id``       The AccessKeyId used to access Tablestore.
``tablestore.access-key-secret``   The AccessKeySecret used to access Tablestore.
``tablestore.instance``            The name of the instance we'd like to connect to.
================================== ===================================================

``tablestore.endpoint``
^^^^^^^^^^^^^^^^^^^^^^^

The endpoint of tablestore, e.g. https://demo.cn-hangzhou.ots.aliyuncs.com.

``tablestore.access-key-id``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The AccessKeyId used to access Tablestore.

``tablestore.access-key-secret``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The AccessKeySecret used to access Tablestore.


``tablestore.instance``
^^^^^^^^^^^^^^^^^^^^^^^

The name of the instance we'd like to connect to.

Data Types
----------

The data type mappings are as follows:

=============== =============
Tablestore      Presto
=============== =============
``BOOLEAN``     ``BOOLEAN``
``INTEGER``     ``INTEGER``
``DOUBLE``      ``DOUBLE``
``VARCHAR``     ``VARCHAR``
``BINARY``      ``VARBINARY``
=============== =============
