===================
LDAP Authentication
===================

Presto can be configured to enable frontend LDAP authentication over
HTTPS for clients, such as the :ref:`cli_ldap`, or the JDBC and ODBC
drivers. At present only simple LDAP authentication mechanism involving
username and password is supported. The Presto client sends a username
and password to the coordinator and coordinator validates these
credentials using an external LDAP service.

To enable LDAP authentication for Presto, configuration changes are made on
the Presto coordinator. No changes are required to the worker configuration;
only the communication from the clients to the coordinator is authenticated.
However, if you want to secure the communication between
Presto nodes with SSL/TLS configure :doc:`/security/internal-communication`.

Presto Server Configuration
---------------------------

Environment Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

.. _ldap_server:

Secure LDAP
~~~~~~~~~~~

Presto requires Secure LDAP (LDAPS), so make sure you have TLS
enabled on your LDAP server.

TLS Configuration on Presto Coordinator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You need to import the LDAP server's TLS certificate to the default Java
truststore of the Presto coordinator to secure TLS connection. You can use
the following example `keytool` command to import the certificate
``ldap_server.crt``, to the truststore on the coordinator.

.. code-block:: none

    $ keytool -import -keystore <JAVA_HOME>/jre/lib/security/cacerts -trustcacerts -alias ldap_server -file ldap_server.crt

In addition to this, access to the Presto coordinator should be
through HTTPS. You can do it by creating a :ref:`server_java_keystore` on
the coordinator.

Presto Coordinator Node Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You must make the following changes to the environment prior to configuring the
Presto coordinator to use LDAP authentication and HTTPS.

 * :ref:`ldap_server`
 * :ref:`server_java_keystore`

You also need to make changes to the Presto configuration files.
LDAP authentication is configured on the coordinator in two parts.
The first part is to enable HTTPS support and password authentication
in the coordinator's ``config.properties`` file. The second part is
to configure LDAP as the password authenticator plugin.

Server Config Properties
~~~~~~~~~~~~~~~~~~~~~~~~

The following is an example of the required properties that need to be added
to the coordinator's ``config.properties`` file:

.. code-block:: none

    http-server.authentication.type=PASSWORD

    http-server.https.enabled=true
    http-server.https.port=8443

    http-server.https.keystore.path=/etc/presto_keystore.jks
    http-server.https.keystore.key=keystore_password

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``http-server.authentication.type``                     Enable password authentication for the Presto
                                                        coordinator. Must be set to ``PASSWORD``.
``http-server.https.enabled``                           Enables HTTPS access for the Presto coordinator.
                                                        Should be set to ``true``. Default value is
                                                        ``false``.
``http-server.https.port``                              HTTPS server port.
``http-server.https.keystore.path``                     The location of the Java Keystore file that will be
                                                        used to secure TLS.
``http-server.https.keystore.key``                      The password for the keystore. This must match the
                                                        password you specified when creating the keystore.
======================================================= ======================================================

Password Authenticator Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Password authentication needs to be configured to use LDAP. Create an
``etc/password-authenticator.properties`` file on the coordinator. Example:

.. code-block:: none

    password-authenticator.name=ldap
    ldap.url=ldaps://ldap-server:636
    ldap.user-bind-pattern=<Refer below for usage>

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``ldap.url``                                            The url to the LDAP server. The url scheme must be
                                                        ``ldaps://`` since Presto allows only Secure LDAP.
``ldap.user-bind-pattern``                              This property can be used to specify the LDAP user
                                                        bind string for password authentication. This property
                                                        must contain the pattern ``${USER}`` which will be
                                                        replaced by the actual username during the password
                                                        authentication. Example: ``${USER}@corp.example.com``.
======================================================= ======================================================

Based on the LDAP server implementation type, the property
``ldap.user-bind-pattern`` can be used as described below.

Active Directory
****************

.. code-block:: none

    ldap.user-bind-pattern=${USER}@<domain_name_of_the_server>

Example:

.. code-block:: none

    ldap.user-bind-pattern=${USER}@corp.example.com

OpenLDAP
********

.. code-block:: none

    ldap.user-bind-pattern=uid=${USER},<distinguished_name_of_the_user>

Example:

.. code-block:: none

    ldap.user-bind-pattern=uid=${USER},OU=America,DC=corp,DC=example,DC=com

Authorization based on LDAP Group Membership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can further restrict the set of users allowed to connect to the Presto
coordinator based on their group membership by setting the optional
``ldap.group-auth-pattern`` and ``ldap.user-base-dn`` properties in addition
to the basic LDAP authentication properties.

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``ldap.user-base-dn``                                   The base LDAP distinguished name for the user
                                                        who tries to connect to the server.
                                                        Example: ``OU=America,DC=corp,DC=example,DC=com``
``ldap.group-auth-pattern``                             This property is used to specify the LDAP query for
                                                        the LDAP group membership authorization. This query
                                                        will be executed against the LDAP server and if
                                                        successful, the user will be authorized.
                                                        This property must contain a pattern ``${USER}``
                                                        which will be replaced by the actual username in
                                                        the group authorization search query.
                                                        See samples below.
======================================================= ======================================================

Based on the LDAP server implementation type, the property
``ldap.group-auth-pattern`` can be used as described below.

Active Directory
****************

.. code-block:: none

    ldap.group-auth-pattern=(&(objectClass=<objectclass_of_user>)(sAMAccountName=${USER})(memberof=<dn_of_the_authorized_group>))

Example:

.. code-block:: none

    ldap.group-auth-pattern=(&(objectClass=person)(sAMAccountName=${USER})(memberof=CN=AuthorizedGroup,OU=Asia,DC=corp,DC=example,DC=com))

OpenLDAP
********

.. code-block:: none

    ldap.group-auth-pattern=(&(objectClass=<objectclass_of_user>)(uid=${USER})(memberof=<dn_of_the_authorized_group>))

Example:

.. code-block:: none

    ldap.group-auth-pattern=(&(objectClass=inetOrgPerson)(uid=${USER})(memberof=CN=AuthorizedGroup,OU=Asia,DC=corp,DC=example,DC=com))

For OpenLDAP, for this query to work, make sure you enable the
``memberOf`` `overlay <http://www.openldap.org/doc/admin24/overlays.html>`_.

You can also use this property for scenarios where you want to authorize a user
based on complex group authorization search queries. For example, if you want to
authorize a user belonging to any one of multiple groups (in OpenLDAP), this
property may be set as follows:

.. code-block:: none

    ldap.group-auth-pattern=(&(|(memberOf=CN=normal_group,DC=corp,DC=com)(memberOf=CN=another_group,DC=com))(objectClass=inetOrgPerson)(uid=${USER}))

.. _cli_ldap:

Presto CLI
----------

Environment Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

TLS Configuration
~~~~~~~~~~~~~~~~~

Access to the Presto coordinator should be through HTTPS when using LDAP
authentication. The Presto CLI can use either a :ref:`Java Keystore
<server_java_keystore>` file or :ref:`Java Truststore <cli_java_truststore>`
for its TLS configuration.

If you are using keystore file, it can be copied to the client machine and used
for its TLS configuration. If you are using truststore, you can either use
default java truststores or create a custom truststore on the CLI. We do not
recommend using self-signed certificates in production.

Presto CLI Execution
^^^^^^^^^^^^^^^^^^^^

In addition to the options that are required when connecting to a Presto
coordinator that does not require LDAP authentication, invoking the CLI
with LDAP support enabled requires a number of additional command line
options. You can either use ``--keystore-*`` or ``--truststore-*`` properties
to secure TLS connection. The simplest way to invoke the CLI is with a
wrapper script.

.. code-block:: none

    #!/bin/bash

    ./presto \
    --server https://presto-coordinator.example.com:8443 \
    --keystore-path /tmp/presto.jks \
    --keystore-password password \
    --truststore-path /tmp/presto_truststore.jks \
    --truststore-password password \
    --catalog <catalog> \
    --schema <schema> \
    --user <LDAP user> \
    --password

=============================== =========================================================================
Option                          Description
=============================== =========================================================================
``--server``                    The address and port of the Presto coordinator.  The port must
                                be set to the port the Presto coordinator is listening for HTTPS
                                connections on. Presto CLI does not support using ``http`` scheme for
                                the url when using LDAP authentication.
``--keystore-path``             The location of the Java Keystore file that will be used
                                to secure TLS.
``--keystore-password``         The password for the keystore. This must match the
                                password you specified when creating the keystore.
``--truststore-path``           The location of the Java Truststore file that will be used
                                to secure TLS.
``--truststore-password``       The password for the truststore. This must match the
                                password you specified when creating the truststore.
``--user``                      The LDAP username. For Active Directory this should be your
                                ``sAMAccountName`` and for OpenLDAP this should be the ``uid`` of
                                the user. This is the username which will be
                                used to replace the ``${USER}`` placeholder pattern in the properties
                                specified in ``config.properties``.
``--password``                  Prompts for a password for the ``user``.
=============================== =========================================================================

Troubleshooting
---------------

Java Keystore File Verification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Verify the password for a keystore file and view its contents using
:ref:`troubleshooting_keystore`.

SSL Debugging for Presto CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you encounter any SSL related errors when running Presto CLI, you can run CLI using ``-Djavax.net.debug=ssl``
parameter for debugging. You should use the Presto CLI executable jar to enable this. Eg:

.. code-block:: none

    java -Djavax.net.debug=ssl \
    -jar \
    presto-cli-<version>-executable.jar \
    --server https://coordinator:8443 \
    <other_cli_arguments>

Common SSL errors
~~~~~~~~~~~~~~~~~

java.security.cert.CertificateException: No subject alternative names present
*****************************************************************************

This error is seen when the Presto coordinator’s certificate is invalid and does not have the IP you provide
in the ``--server`` argument of the CLI. You will have to regenerate the coordinator's SSL certificate
with the appropriate :abbr:`SAN (Subject Alternative Name)` added.

Adding a SAN to this certificate is required in cases where ``https://`` uses IP address in the URL rather
than the domain contained in the coordinator's certificate, and the certificate does not contain the
:abbr:`SAN (Subject Alternative Name)` parameter with the matching IP address as an alternative attribute.
