================
Thrift Connector
================

The Thrift connector makes it possible to integrate with external storage systems
without a custom Presto connector implementation.

In order to use the Thrift connector with an external system, you need to implement
the ``PrestoThriftService`` interface, found below. Next, you configure the Thrift connector
to point to a set of machines, called Thrift servers, that implement the interface.
As part of the interface implementation, the Thrift servers will provide metadata,
splits and data. The connector will randomly choose a server to talk to from the available
instances for metadata calls, or for data calls unless the splits include a list of addresses.
All requests are assumed to be idempotent and can be retried freely among any server.

Configuration
-------------

To configure the Thrift connector, create a catalog properties file
``etc/catalog/thrift.properties`` with the following content,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=presto-thrift
    presto.thrift.client.addresses=host:port,host:port

Multiple Thrift Systems
^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Thrift systems to connect to, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``).

Configuration Properties
------------------------

The following configuration properties are available:

=============================================  ==============================================================
Property Name                                  Description
=============================================  ==============================================================
``presto.thrift.client.addresses``             Location of Thrift servers
``presto-thrift.max-response-size``            Maximum size of data returned from Thrift server
``presto-thrift.metadata-refresh-threads``     Number of refresh threads for metadata cache
``presto.thrift.client.max-retries``           Maximum number of retries for failed Thrift requests
``presto.thrift.client.max-backoff-delay``     Maximum interval between retry attempts
``presto.thrift.client.min-backoff-delay``     Minimum interval between retry attempts
``presto.thrift.client.max-retry-time``        Maximum duration across all attempts of a Thrift request
``presto.thrift.client.backoff-scale-factor``  Scale factor for exponential back off
``presto.thrift.client.connect-timeout``       Connect timeout
``presto.thrift.client.request-timeout``       Request timeout
``presto.thrift.client.socks-proxy``           SOCKS proxy address
``presto.thrift.client.max-frame-size``        Maximum size of a raw Thrift response
``presto.thrift.client.transport``             Thrift transport type (``UNFRAMED``, ``FRAMED``, ``HEADER``)
``presto.thrift.client.protocol``              Thrift protocol type (``BINARY``, ``COMPACT``, ``FB_COMPACT``)
=============================================  ==============================================================

``presto.thrift.client.addresses``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of thrift servers in the form of ``host:port``. For example:

.. code-block:: none

    presto.thrift.client.addresses=192.0.2.3:7777,192.0.2.4:7779

This property is required; there is no default.

``presto-thrift.max-response-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Maximum size of a data response that the connector accepts. This value is sent
by the connector to the Thrift server when requesting data, allowing it to size
the response appropriately.

This property is optional; the default is ``16MB``.

``presto-thrift.metadata-refresh-threads``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Number of refresh threads for metadata cache.

This property is optional; the default is ``1``.

Thrift IDL File
---------------

The following IDL describes the ``PrestoThriftService`` that must be implemented:

.. literalinclude:: /include/PrestoThriftService.thrift
    :language: thrift
