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

``PrestoAuthenticator`` contains a single method, ``createAuthenticatedPrincipal(Map<String, List<String>> headers)``,
that validates the request headers and returns a ``Principal``, which is then
authorized by the :doc:`system-access-control`.

The implementation of ``PrestoAuthenticatorFactory`` must be wrapped
as a plugin and installed on the Presto cluster.

Error Handling
--------------

The ``createAuthenticatedPrincipal(Map<String, List<String>> headers)`` method can throw two types of exceptions,
depending on the authentication outcome:

* ``AuthenticatorNotApplicableException``:

  Thrown when the required authentication header is missing or invalid. This signals
  to Presto that the current authentication method is not applicable, so it should
  skip this authenticator and try the next configured one. The exception message is
  not returned to the user, since authentication was never intended for this request.

* ``AccessDeniedException``:

  Thrown when the required header is present but authentication fails. In this case,
  Presto will still try the next configured authenticator but the error message is
  passed back to the user, indicating that the authentication attempt was valid but
  unsuccessful.

This distinction ensures that Presto can properly chain multiple authenticators
while providing meaningful feedback to the user only when appropriate.

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

