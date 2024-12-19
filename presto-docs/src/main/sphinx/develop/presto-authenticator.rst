===========================
Custom Presto Authenticator
===========================

Presto supports authentication through a custom Presto authenticator
that validates the request and creates a principal.

Implementation
--------------

``PrestoAuthenticatorFactory`` creates a
``PrestoAuthenticator`` instance. It also defines the name of this
authenticator which is used by the administrator in a Presto configuration.

``PrestoAuthenticator`` contains a single method, ``createAuthenticatedPrincipal()``,
that validates the request and returns a ``Principal``, which is then
authorized by the :doc:`system-access-control`.

The implementation of ``PrestoAuthenticatorFactory`` must be wrapped
as a plugin and installed on the Presto cluster.

Configuration
-------------

After a plugin that implements ``PrestoAuthenticatorFactory`` has been
installed on the coordinator, it is configured using an
``etc/presto-authenticator.properties`` file. All of the
properties other than ``presto-authenticator.name`` are specific to the
``PrestoAuthenticatorFactory`` implementation.

The ``presto-authenticator.name`` property is used by Presto to find a
registered ``PrestoAuthenticatorFactory`` based on the name returned by
``PrestoAuthenticatorFactory.getName()``. The remaining properties are
passed as a map to ``PrestoAuthenticatorFactory.create()``.

Example configuration file:

.. code-block:: none

    presto-authenticator.name=custom-authenticator
    custom-property1=custom-value1
    custom-property2=custom-value2

Additionally, the coordinator must be configured to use custom authentication
and have HTTPS enabled.

Add the property shown below to the coordinator's ``config.properties`` file:

.. code-block:: none

    http-server.authentication.type=CUSTOM

