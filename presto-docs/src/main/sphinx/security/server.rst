===================================
Coordinator Kerberos Authentication
===================================

The Presto coordinator can be configured to enable Kerberos authentication over
HTTPS for clients, such as the :doc:`Presto CLI </security/cli>`, or the
JDBC and ODBC drivers.

To enable Kerberos authentication for Presto, configuration changes are made on
the Presto coordinator. No changes are required to the worker configuration;
the worker nodes will continue to connect to the coordinator over
unauthenticated HTTP. However, if you want to secure the communication between
Presto nodes with SSL/TLS, configure :doc:`/security/internal-communication`.


Environment Configuration
-------------------------

.. |subject_node| replace:: Presto coordinator

.. _server_kerberos_services:
.. include:: kerberos-services.fragment

.. _server_kerberos_configuration:
.. include:: kerberos-configuration.fragment

.. _server_kerberos_principals:

Kerberos Principals and Keytab Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Presto coordinator needs a Kerberos principal, as do users who are going to
connect to the Presto coordinator. You will need to create these users in
Kerberos using `kadmin
<http://web.mit.edu/kerberos/krb5-latest/doc/admin/admin_commands/kadmin_local.html>`_.

In addition, the Presto coordinator needs a `keytab file
<http://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html>`_. After you create the principal, you can create the keytab file using :command:`kadmin`

.. code-block:: none

    kadmin
    > addprinc -randkey presto@EXAMPLE.COM
    > addprinc -randkey presto/presto-coordinator.example.com@EXAMPLE.COM
    > ktadd -k /etc/presto/presto.keytab presto@EXAMPLE.COM
    > ktadd -k /etc/presto/presto.keytab presto/presto-coordinator.example.com@EXAMPLE.COM

.. include:: ktadd-note.fragment

.. include:: jce-policy.fragment

Java Keystore File for TLS
^^^^^^^^^^^^^^^^^^^^^^^^^^
When using Kerberos authentication, access to the Presto coordinator should be
through HTTPS. You can do it by creating a :ref:`server_java_keystore` on the
coordinator.

System Access Control Plugin
----------------------------

A Presto coordinator with Kerberos enabled will probably need a
:doc:`/develop/system-access-control` plugin to achieve
the desired level of security.

Presto Coordinator Node Configuration
-------------------------------------

You must make the above changes to the environment prior to configuring the
Presto coordinator to use Kerberos authentication and HTTPS. After making the
following environment changes, you can make the changes to the Presto
configuration files.

* :ref:`server_kerberos_services`
* :ref:`server_kerberos_configuration`
* :ref:`server_kerberos_principals`
* :ref:`server_java_keystore`
* :doc:`System Access Control Plugin </develop/system-access-control>`

config.properties
^^^^^^^^^^^^^^^^^

Kerberos authentication is configured in the coordinator node's
:file:`config.properties` file. The entries that need to be added are listed below.

.. code-block:: none

    http-server.authentication.type=KERBEROS

    http.server.authentication.krb5.service-name=presto
    http.server.authentication.krb5.service-hostname=presto.example.com
    http.server.authentication.krb5.keytab=/etc/presto/presto.keytab
    http.authentication.krb5.config=/etc/krb5.conf

    http-server.https.enabled=true
    http-server.https.port=7778

    http-server.https.keystore.path=/etc/presto_keystore.jks
    http-server.https.keystore.key=keystore_password

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``http-server.authentication.type``                     Authentication type for the Presto
                                                        coordinator. Must be set to ``KERBEROS``.
``http.server.authentication.krb5.service-name``        The Kerberos service name for the Presto coordinator.
                                                        Must match the Kerberos principal.
``http.server.authentication.krb5.principal-hostname``  The Kerberos hostname for the Presto coordinator.
                                                        Must match the Kerberos principal. This parameter is
                                                        optional. If included, Presto will use this value
                                                        in the host part of the Kerberos principal instead
                                                        of the machine's hostname.
``http.server.authentication.krb5.keytab``              The location of the keytab that can be used to
                                                        authenticate the Kerberos principal.
``http.authentication.krb5.config``                     The location of the Kerberos configuration file.
``http-server.https.enabled``                           Enables HTTPS access for the Presto coordinator.
                                                        Should be set to ``true``.
``http-server.https.port``                              HTTPS server port.
``http-server.https.keystore.path``                     The location of the Java Keystore file that will be
                                                        used to secure TLS.
``http-server.https.keystore.key``                      The password for the keystore. This must match the
                                                        password you specified when creating the keystore.
======================================================= ======================================================

.. note::

    Monitor CPU usage on the Presto coordinator after enabling HTTPS. Java
    prefers the more CPU-intensive cipher suites if you allow it to choose from
    a big list. If the CPU usage is unacceptably high after enabling HTTPS,
    you can configure Java to use specific cipher suites by setting
    the ``http-server.https.included-cipher`` property to only allow
    cheap ciphers. Non forward secrecy (FS) ciphers are disabled by default.
    As a result, if you want to choose non FS ciphers, you need to set the
    ``http-server.https.excluded-cipher`` property to an empty list in order to
    override the default exclusions.

    .. code-block:: none

        http-server.https.included-cipher=TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256
        http-server.https.excluded-cipher=

    The Java documentation lists the `supported cipher suites
    <http://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SupportedCipherSuites>`_.

access-controls.properties
^^^^^^^^^^^^^^^^^^^^^^^^^^

At a minimum, an :file:`access-control.properties` file must contain an
``access-control.name`` property.  All other configuration is specific
for the implementation being configured.
See :doc:`/develop/system-access-control` for details.

.. _coordinator-troubleshooting:

Troubleshooting
---------------

Getting Kerberos authentication working can be challenging. You can
independently verify some of the configuration outside of Presto to help narrow
your focus when trying to solve a problem.

Kerberos Verification
^^^^^^^^^^^^^^^^^^^^^

Ensure that you can connect to the KDC from the Presto coordinator using
:command:`telnet`.

.. code-block:: none

    $ telnet kdc.example.com 88

Verify that the keytab file can be used to successfully obtain a ticket using
`kinit
<http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html>`_ and
`klist
<http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/klist.html>`_

.. code-block:: none

    $ kinit -kt /etc/presto/presto.keytab presto@EXAMPLE.COM
    $ klist

Java Keystore File Verification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Verify the password for a keystore file and view its contents using
:ref:`troubleshooting_keystore`

Additional Kerberos Debugging Information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can enable additional Kerberos debugging information for the Presto
coordinator process by adding the following lines to the Presto ``jvm.config``
file

.. code-block:: none

    -Dsun.security.krb5.debug=true
    -Dlog.enable-console=true

``-Dsun.security.krb5.debug=true`` enables Kerberos debugging output from the
JRE Kerberos libraries. The debugging output goes to ``stdout``, which Presto
redirects to the logging system. ``-Dlog.enable-console=true`` enables output
to ``stdout`` to appear in the logs.

The amount and usefulness of the information the Kerberos debugging output
sends to the logs varies depending on where the authentication is failing.
Exception messages and stack traces can also provide useful clues about the
nature of the problem.

.. _server_additional_resources:

Additional resources
^^^^^^^^^^^^^^^^^^^^

`Common Kerberos Error Messages (A-M)
<http://docs.oracle.com/cd/E19253-01/816-4557/trouble-6/index.html>`_

`Common Kerberos Error Messages (N-Z)
<http://docs.oracle.com/cd/E19253-01/816-4557/trouble-27/index.html>`_

`MIT Kerberos Documentation: Troubleshooting
<http://web.mit.edu/kerberos/krb5-latest/doc/admin/troubleshoot.html>`_
