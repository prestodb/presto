=============
Authorization
=============

Presto can be configured to enable authorization support for HTTP endpoints to
allow system administrators to control access to different HTTP endpoints in
Presto.

Role Based Access Control
-------------------------

Every HTTP endpoint in Presto is secured by a role list and can only be accessed
by the role in the list. There are three roles defined in Presto:

* ``user``: Users who should have access to external endpoints like those needed to launch queries, check status, get output data and provides data for UI.
* ``internal``: Internal components of Presto (like coordinator and worker) which will have access to endpoints like launching tasks on workers and fetching exchange data from another worker.
* ``admin``: System administrators who will have access to internal service endpoints like those to get node status.

Enabling Authorization
----------------------

The following steps need to be taken in order to enable authorization:

 1. :ref:`enable_authentication`
 2. :ref:`configure_authorizer`
 3. :ref:`configure_authorization_settings`

.. _enable_authentication:

Enable Authentication
^^^^^^^^^^^^^^^^^^^^^

Presto authorization requires authentication to get the accessor's principal,
so make sure you have authentication enabled.

   - If TLS/SSL is configured properly, we can just use the certificate to
     identify the accessor.

     .. code-block:: none

         http-server.authentication.type=CERTIFICATE

   - It is also possible to specify other authentication types such as
     ``KERBEROS``, ``PASSWORD`` and ``JWT``. Additional configuration may be
     needed.

     .. code-block:: none

         node.internal-address=<authentication type>

.. _configure_authorizer:

Configure Authorizer
^^^^^^^^^^^^^^^^^^^^

To enable authorization, the interface
``com.facebook.airlift.http.server.Authorizer`` must be implemented and bound.
It performs the actual authorization check based on the principal of incoming
request and the allowed roles of endpoint being requested, determines if the
principal belongs to at least one of the allowed roles.

You can either use the preset ``ConfigurationBasedAuthorizer`` or implement
your own.

Configuration-based Authorizer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This plugin allows you to turn on authorization support by specifying an
identity regex file. To use this plugin, add an ``etc/roles.properties`` file
containing regexes for each allowed role.

Identity Regex
~~~~~~~~~~~~~~

The principal is granted role(s) based on the matching regex(es) read from the
identity regex file. If no role matches, access is denied.

For example, if you want all principals to have the permission to access the
endpoints who allow ``USER``, only allow the principal coordinator to access
``INTERNAL`` endpoints, and the ones starting with "su" to access ``ADMIN``
endpoints. You can add an ``etc/roles.properties`` with the following contents:

.. code-block:: none

    user=.*
    internal=coordinator
    admin=su.*

Specify File Path
~~~~~~~~~~~~~~~~~

The following property needs to be added to the ``config.properties`` file:

.. code-block:: none

    configuration-based-authorizer.role-regex-map.file-path=etc/roles.properties

Install Module
~~~~~~~~~~~~~~

Install ``com.facebook.airlift.http.server.ConfigurationBasedAuthorizerModule``
in Presto. It binds the preset configuration-based authorizer and its
properties.

.. _configure_authorization_settings:

Configure Authorization Settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Authorization settings is configured in the ``config.properties`` file. The
authorization on the worker and coordinator nodes are configured using the same
set of properties.

The following is an example of the properties that need to be added to the
``config.properties`` file:

.. code-block:: none

    http-server.authorization.enabled=true
    http-server.authorization.default-policy=ALLOW
    http-server.authorization.default-allowed-roles=USER,ADMIN
    http-server.authorization.allow-unsecured-requests=false

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``http-server.authorization.enabled``                   Enable authorization for the Presto.
                                                        Should be set to ``true``. Default value is
                                                        ``false``.
``http-server.authorization.default-policy``            The default authorization policy applies to endpoints
                                                        without allowed roles specified. Can be set to
                                                        ``ALLOW``, ``DENY`` and ``DEFAULT_ROLES``.
``http-server.authorization.default-allowed-roles``     The roles allowed to access the endpoints without
                                                        explicitly specified when default-policy is set to
                                                        ``DEFAULT_ROLES``.
``http-server.authorization.allow-unsecured-requests``  Skip authorization check for unsecured requests.
                                                        Default value is ``false``.
======================================================= ======================================================

.. warning::

    ``http-server.authorization.allow-unsecured-requests`` is provided as a way to
    transition from HTTP to HTTPS with authorization and is a security hole
    since it allows unauthenticated requests to skip authorization checks. Only
    enable during the transition period and disable this setting once done.
