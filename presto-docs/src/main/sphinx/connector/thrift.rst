================
Thrift Connector
================

The Thrift connector makes it possible to integrate with external storage systems
without a custom Presto connector implementation.

In order to use the Thrift connector with an external system, you need to implement
the ``PrestoThriftService`` interface, found below. Next, you configure the Thrift connector
to point to a set of machines, called Thrift servers, that implement the interface.
As part of the interface implementation, the Thrift servers will provide metadata,
splits and data. The Thrift server instances are assumed to be stateless and independent
from each other.

Configuration
-------------

To configure the Thrift connector, create a catalog properties file
``etc/catalog/thrift.properties`` with the following content,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=presto-thrift
    static-location.hosts=host:port,host:port

Multiple Thrift Systems
^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Thrift systems to connect to, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``).

Configuration Properties
------------------------

The following configuration properties are available:

===========================================   ==============================================================
Property Name                                 Description
===========================================   ==============================================================
``static-location.hosts``                     Location of Thrift servers
``presto-thrift.max-response-size``           Maximum size of a response from thrift server
``presto-thrift.metadata-refresh-threads``    Number of refresh threads for metadata cache
===========================================   ==============================================================

``static-location.hosts``
^^^^^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of thrift servers in the form of ``host:port``. For example:

.. code-block:: none

    static-location.hosts=192.0.2.3:7777,192.0.2.4:7779

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

Thrift Client Properties
^^^^^^^^^^^^^^^^^^^^^^^^

The following properties allow configuring the Thrift client used by the connector:

=====================================================   ===================   =============
Property Name                                           Description           Default Value
=====================================================   ===================   =============
``PrestoThriftService.thrift.client.connect-timeout``   Connect timeout       ``500ms``
``PrestoThriftService.thrift.client.max-frame-size``    Max frame size        ``16MB``
``PrestoThriftService.thrift.client.read-timeout``      Read timeout          ``10s``
``PrestoThriftService.thrift.client.receive-timeout``   Receive timeout       ``1m``
``PrestoThriftService.thrift.client.socks-proxy``       Socks proxy address   ``null``
``PrestoThriftService.thrift.client.write-timeout``     Write timeout         ``1m``
=====================================================   ===================   =============

Thrift IDL File
---------------

The following IDL describes the ``PrestoThriftService`` that must be implemented:

.. literalinclude:: /include/PrestoThriftService.thrift
    :language: thrift
