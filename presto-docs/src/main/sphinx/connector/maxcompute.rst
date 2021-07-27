====================
MaxCompute Connector
====================

Overview
--------

The MaxCompute Connector allows access to Alibaba Cloud MaxCompute data from Presto.
This document describes how to setup the MaxCompute Connector to run SQL queries against Alibaba MaxCompute.

Configuration
-------------

To configure the MaxCompute connector, create a catalog properties file
``etc/catalog/maxcompute.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=maxcompute
    maxcompute.endpoint=http://service.cn-hangzhou.maxcompute.aliyun.com/api
    maxcompute.tunnel-url=http://dt.cn-hangzhou.maxcompute.aliyun.com
    maxcompute.access-key-id=alicloud-access-key-id
    maxcompute.access-key-secret=alicloud-access-key-secret
    maxcompute.default-project=maxcompute-default-project

Configuration Properties
------------------------

The following configuration properties are available:

``maxcompute.endpoint``
^^^^^^^^^^^^^^^^^^^^^^^

MaxCompute endpoint: used to send all requests except for data upload and download requests to MaxCompute.

``maxcompute.endpoint``
^^^^^^^^^^^^^^^^^^^^^^^

MaxCompute Tunnel endpoint: used to upload data to and download data from MaxCompute.

``maxcompute.access-key-id``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The AccessKeyId used to access MaxCompute.

``maxcompute.access-key-secret``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The AccessKeySecret used to access MaxCompute.

``maxcompute.default-project``
^^^^^^^^^^^^^^^^^^^^^^^

The default project used when connected to MaxCompute.

``maxcompute.load-splits-threads``
^^^^^^^^^^^^^^^^^^^^^^^

The number of concurrent threads used to load splits.
The default is Node CPUs * 2.

``maxcompute.rows-per-split``
^^^^^^^^^^^^^^^^^^^^^^^

Max row count in a split.
The default is 500000.

``maxcompute.partition-cache-threads``
^^^^^^^^^^^^^^^^^^^^^^^

The number of concurrent threads used to load partition metadata cache.
The default is 2.

Querying MaxCompute
-----------------

The MaxCompute connector provides access to all projects visible to the specified access-key-id. Each MaxCompute project
is represented as a presto schema.
For the following examples, assume the MaxCompute catalog is ``maxcompute``.

You can see the available schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM maxcompute;

If you have a MaxCompute schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM maxcompute.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE maxcompute.web.clicks;
    SHOW COLUMNS FROM maxcompute.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM maxcompute.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``maxcompute`` in the above examples.

MaxCompute Connector Limitations
------------------------------

The following SQL statements are not yet supported:

* :doc:`/sql/delete`
* :doc:`/sql/alter-table`
* :doc:`/sql/create-table`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
