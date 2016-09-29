====================
Local File Connector
====================

The local file connector allows querying data stored on the local
file system of each worker.

Configuration
-------------

To configure the local file connector, create a catalog properties file
under ``etc/catalog`` named, for example, ``localfile.properties`` with the following contents:

.. code-block:: none

    connector.name=localfile

Configuration Properties
------------------------

=========================================   ==============================================================
Property Name                               Description
=========================================   ==============================================================
``presto-logs.http-request-log.location``   Directory or file where HTTP request logs are written
``presto-logs.http-request-log.pattern``    If the log location is a directory this glob is used
                                            to match file names in the directory
=========================================   ==============================================================

Local File Connector Schemas and Tables
---------------------------------------

The local file connector provides a single schema named ``logs``.
You can see all the available tables by running ``SHOW TABLES``::

    SHOW TABLES FROM localfile.logs;

``http_request_log``
^^^^^^^^^^^^^^^^^^^^
This table contains the HTTP request logs from each node on the cluster.
