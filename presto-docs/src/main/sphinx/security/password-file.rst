.. _password_file_auth:

============================
Password File Authentication
============================

Presto can be configured to enable frontend password authentication over
HTTPS for clients, such as the :ref:`cli`, or the JDBC and ODBC drivers. The
username and password are validated against usernames and passwords stored
in a file. Once password-file-based authentication is set up, no user is able
to connect to the Presto coordinator without authenticating themselves.

To enable password-file-based authentication for Presto, configuration changes are made on
the Presto coordinator. No changes are required to the worker configuration;
only the communication from the clients to the coordinator is authenticated.
To secure the communication between Presto nodes with SSL/TLS, see :doc:`/security/internal-communication`.

Presto Server Configuration
---------------------------

TLS Configuration on Presto Coordinator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using password-file-based authentication, access to the Presto coordinator
should be through HTTPS. With the HTTPS protocol, the Presto coordinator needs to present its
certificate to the client for successfully completing SSL handshake. You can do it by
creating a :ref:`server_java_keystore` on the coordinator.

Presto Coordinator Node Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You must make the following changes to the environment prior to configuring the
Presto coordinator to use password-file-based authentication and HTTPS.

* :ref:`server_java_keystore`

You also need to make changes to the Presto configuration files.
Password-file-based authentication is configured on the coordinator in two parts.
The first part is to enable HTTPS support and password authentication
in the coordinator's ``config.properties`` file. The second part is
to configure the password file as the password authenticator.

Server Config Properties
~~~~~~~~~~~~~~~~~~~~~~~~

The following is an example of the properties that must be added
to the coordinator's ``config.properties`` file:

.. code-block:: none

    http-server.authentication.type=PASSWORD

    http-server.https.enabled=true
    http-server.https.port=8443

    http-server.https.keystore.path=/etc/presto_keystore.jks
    http-server.https.keystore.key=keystore_password

.. note::

    If :doc:`/security/internal-communication` is not configured, you need to keep
    both ``http-server.http.port`` and ``http-server.https.port`` in your ``config.properties``.
    Further, no changes are needed in the value for ``discovery.uri``.


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
``http-server.authentication.allow-forwarded-https``    Enable treating forwarded HTTPS requests over HTTP as secure.
                                                        Requires the ``X-Forwarded-Proto`` header to be set to ``https`` on forwarded requests.
                                                        Default value is ``false``.
======================================================= ======================================================

Password Authenticator Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Enable password file authentication by creating an
``etc/password-authenticator.properties`` file on the coordinator:

.. code-block:: none

    password-authenticator.name=file
    file.password-file=/path/to/password.db

The following configuration properties are available:

==================================== ==============================================
Property                             Description
==================================== ==============================================
``file.password-file``               Path of the password file.

``file.refresh-period``              How often to reload the password file.
                                     Defaults to ``5s``.

``file.auth-token-cache.max-size``   Max number of cached authenticated passwords.
                                     Defaults to ``1000``.
==================================== ==============================================

Password Validation
-------------------

Password validation in Presto supports both `PBKDF2WithHmacSHA256` and `PBKDF2WithHmacSHA1` algorithms. 
To ensure modern cryptographic standards, clients are encouraged to use `PBKDF2WithHmacSHA256`. 
A fallback mechanism is available to maintain compatibility with legacy systems using `PBKDF2WithHmacSHA1`.

Migration to `PBKDF2WithHmacSHA256` is strongly recommended to maintain security.

API Method
^^^^^^^^^^
The following method uses the `PBKDF2WithHmacSHA256` validation mechanism and includes a fallback mechanism:

.. code-block:: java

    /**
     * @Deprecated using PBKDF2WithHmacSHA1 is deprecated and clients should switch to PBKDF2WithHmacSHA256
     */
    public static boolean doesPBKDF2PasswordMatch(String inputPassword, String hashedPassword)
    {
        PBKDF2Password password = PBKDF2Password.fromString(hashedPassword);

        // Validate using PBKDF2WithHmacSHA256
        if (validatePBKDF2Password(inputPassword, password, "PBKDF2WithHmacSHA256")) {
            return true;
        }

        // Fallback to PBKDF2WithHmacSHA1
        LOG.warn("Using deprecated PBKDF2WithHmacSHA1 for password validation.");
        return validatePBKDF2Password(inputPassword, password, "PBKDF2WithHmacSHA1");
    }

**Fallback Mechanism**

If `PBKDF2WithHmacSHA256` fails for legacy reasons, the system gracefully falls back to `PBKDF2WithHmacSHA1` while logging a warning.

Password Files
--------------

File Format
^^^^^^^^^^^

The password file contains a list of usernames and passwords, one per line,
separated by a colon. Passwords must be securely hashed using bcrypt or PBKDF2.

bcrypt passwords start with ``$2y$`` and must use a minimum cost of ``8``:

.. code-block:: none

    test:$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa

PBKDF2 passwords are composed of the iteration count, followed by the
hex encoded salt and hash:

.. code-block:: none

    test:1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0

Creating a Password File
^^^^^^^^^^^^^^^^^^^^^^^^

Password files utilizing the bcrypt format can be created using the
`htpasswd <https://httpd.apache.org/docs/current/programs/htpasswd.html>`_
utility from the `Apache HTTP Server <https://httpd.apache.org/>`_.
The cost must be specified, as Presto enforces a higher minimum cost
than the default.

Create an empty password file to get started:

.. code-block:: none

    touch password.db

Add or update the password for the user ``test``:

.. code-block:: none

    htpasswd -B -C 10 password.db test

.. _cli:

Presto CLI
----------

See :doc:`/clients/presto-cli` for instructions to set up Presto CLI.

Environment Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

TLS Configuration
~~~~~~~~~~~~~~~~~

Access to the Presto coordinator should be through HTTPS when using
password-file-based authentication. The Presto CLI must verify the certificate presented by the
coordinator using its truststore to complete SSL handshake. You can either use the
default truststore which comes with your Java installation on the client machine, or
you can define a custom truststore. See :ref:`cli_java_truststore` for instructions
to import the server's certificate into the CLI's truststore. We do not recommend using self-signed
certificates in production.

.. _cli_execution:

Presto CLI Execution
^^^^^^^^^^^^^^^^^^^^

In addition to the options that are required when connecting to a Presto
coordinator that does not require authentication, invoking the CLI
for secure Presto cluster requires a number of additional command line
options. You must include ``--truststore-*`` properties to secure the
connection.

.. code-block:: none

    #!/bin/bash

    ./presto \
    --server https://presto-coordinator.example.com:8443 \
    --truststore-path /tmp/presto_truststore.jks \
    --truststore-password <password> \
    --user <user> \
    --password

=============================== =========================================================================
Option                          Description
=============================== =========================================================================
``--server``                    The address and port of the Presto coordinator.  The port must
                                be set to the port the Presto coordinator is listening for HTTPS
                                connections on. Presto CLI does not support using ``http`` scheme for
                                the url when using password-file-based authentication.
``--truststore-path``           The location of the Java Truststore file on the client machine that will be used
                                to secure TLS.
``--truststore-password``       The password for the truststore. This must match the
                                password you specified when creating the truststore.
``--user``                      The username of the user trying to connect to the server.
``--password``                  Prompts for a password for the ``user``.
=============================== =========================================================================

.. note::

    Run ``./presto --help`` for getting the list of available command line options with their descriptions.


Troubleshooting
---------------

Java Keystore File Verification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Verify the password for a keystore file and view its contents using
:ref:`troubleshooting_keystore`.

SSL Debugging for Presto CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Debug SSL related errors when running Presto CLI using :ref:`ssl_debugging_for_cli`.

NullPointerException while locating HttpServerProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    [Guice/ErrorInCustomProvider]: NullPointerException
      while locating HttpServerProvider
      at HttpServerModule.configure(HttpServerModule.java:54)
      while locating HttpServer

Include below configurations in ``config.properties`` file

.. code-block:: none

    http-server.https.keystore.path=<path_to_keystore>
    http-server.https.keystore.key=<keystore_password>


Password authenticator file is not registered
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check if ``presto-password-authenticators`` plugin is included with Presto installation.
For local installations, include below line in ``plugin.bundles`` in ``config.properties`` file:

.. code-block:: none

    ../presto-password-authenticators/pom.xml


Authentication using username/password requires HTTPS to be enabled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    Exception in thread "main" java.lang.IllegalArgumentException: Authentication using username/password requires HTTPS to be enabled
            at com.google.common.base.Preconditions.checkArgument(Preconditions.java:143)
            at com.facebook.presto.cli.QueryRunner.setupBasicAuth(QueryRunner.java:166)
            at com.facebook.presto.cli.QueryRunner.<init>(QueryRunner.java:95)
            at com.facebook.presto.cli.Console.run(Console.java:143)
            at com.facebook.presto.cli.Presto.main(Presto.java:31)

Include ``--server`` command line option with HTTPS endpoint when running Presto CLI.

PKIX path building failed
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    Error running command: javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target

Import the Presto coordinator's certificate into client's truststore. See :ref:`cli_java_truststore` for instructions to import the certificate.
Also make sure to include ``--truststore-*`` properties when running Presto CLI as suggested in :ref:`cli_execution`.

javax.net.ssl.SSLPeerUnverifiedException: Hostname <some_host_name> not verified
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This error implies the Common Name in the imported coordinator's certificate in the client's truststore does not match the hostname
provided with ``--server`` flag. Modify the ``--server`` option to use the same hostname that is mentioned as the Common Name
in the certificate, or create a new certificate for the server with <some_host_name> as the Common Name. See :ref:`server_java_keystore`
for instructions to create a new certificate for the server using :command:`keytool`.

Common SSL Errors
^^^^^^^^^^^^^^^^^

See `SSL Handshake Failures <https://www.baeldung.com/java-ssl-handshake-failures>`_ for detailed
explanation around common SSL errors and their fixes.