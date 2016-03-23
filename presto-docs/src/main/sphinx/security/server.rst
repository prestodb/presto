==========================================
Presto Coordinator Kerberos Authentication
==========================================

The Presto coordinator can be configured to enable Kerberos authentication over
HTTPS for clients, such as the :doc:`Presto CLI </security/cli>`, or the
JDBC and ODBC drivers.

.. warning::

  Worker nodes cannot yet be configured to connect to the Presto coordinator
  using HTTPS or to authenticate with Kerberos. It is the administrator's
  responsibility to enable unauthenticated access over HTTP for worker nodes
  and ensure unathenticated access is blocked for any node that is not a worker
  node. For nodes that are not worker nodes, block access to the Presto
  coordinator's HTTP port.

To enable Kerberos authentication for Presto, configuration changes are made on
the Presto coordinator. No changes are required to the worker configuration;
the worker nodes will continue to connect to the coordinator over
unauthenticated HTTP.

Environment Configuration
-------------------------

.. |subject_node| replace:: Presto coordinator
.. |subject_ref| replace:: server

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

.. _server_java_keystore:

Java Keystore File for TLS
^^^^^^^^^^^^^^^^^^^^^^^^^^

When using Kerberos authentication, access to the Presto coordinator should be
through HTTPS. The Presto coordinator needs keys to secure the :abbr:`TLS
(Transport Layer Security)` connection. These keys are generated using
:command:`keytool` and stored in a Java Keystore file for the Presto
coordinator.

The alias in the :command:`keytool` command line should match the principal that the
Presto coordinator will use.

You'll be prompted for the first and last name. Use the Common Name that will
be used in the certificate. In this case it should be the unqualified hostname
of the Presto coordinator. In the example, you can see this in the prompt that
confirms the information is correct.

.. code-block:: none

  keytool -genkeypair -alias presto -keyalg RSA -keystore keystore.jks
  Enter keystore password:
  Re-enter new password:
  What is your first and last name?
    [Unknown]:  presto-coordinator
  What is the name of your organizational unit?
    [Unknown]:
  What is the name of your organization?
    [Unknown]:
  What is the name of your City or Locality?
    [Unknown]:
  What is the name of your State or Province?
    [Unknown]:
  What is the two-letter country code for this unit?
    [Unknown]:
  Is CN=eiger, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown correct?
    [no]:  yes

  Enter key password for <presto>
          (RETURN if same as keystore password):


.. _server_access_controller:

Access Controller Implementation
--------------------------------

Presto separates the concept of the principal who authenticates to the
coordinator from the username that is responsible for running queries. When
running the Presto CLI, for example, the Presto username can be specified using
the --user option.

By default, the Presto coordinator allows any principal to run queries as any
Presto user. In a secure environment, this is probably not desirable behavior.

This behavior can be customized by implementing the
``SystemAccessControlFactory`` and ``SystemAccessControl`` interfaces.

``SystemAccessControlFactory`` is responsible for creating a
``SystemAccessControl`` instance. It also defines a ``SystemAccessControl``
name which is used by the administrator in a Presto configuration.

``SystemAccessControl`` implementations have two responsibilities:
 * Verifying whether or not a given Kerberos principal is authorized to execute
   queries as a specific user.
 * Determining whether or not a given user can alter values for a given system
   property.

The implementation of ``SystemAccessControl`` and
``SystemAccessControlFactory`` must be wrapped as a plugin and installed on the
Presto cluster.

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
 * :ref:`server_access_controller`

config.properties
^^^^^^^^^^^^^^^^^

Kerberos authentication is configured in the coordinator node's
config.properties file. The entries that need to be added are listed below.

.. code-block:: none

  http.server.authentication.enabled=true

  http.server.authentication.krb5.service-name=presto
  http.server.authentication.krb5.keytab=/etc/presto/presto.keytab
  http.authentication.krb5.config=/etc/krb5.conf

  http-server.https.enabled=true
  http-server.https.port=7778

  http-server.https.keystore.path=/etc/presto_keystore.jks
  http-server.https.keystore.key=keystore_password

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``http.server.authentication.enabled``                  Enable authentication for the Presto coordinator.
                                                        Must be set to ``true``.
``http.server.authentication.krb5.service-name``        The Kerberos server name for the Presto coordinator.
                                                        Must match the Kerberos principal.
``http.server.authentication.krb5.keytab``              The location of the keytab that can be used to
                                                        authenticate the Kerberos principal specified in
                                                        ``http.server.authentication.krb5.service-name``.
``http.authentication.krb5.config``                     The location of the Kerberos configuration file.
``http-server.https.enabled``                           Enables HTTPS access for the Presto coordinator.
                                                        Should be set to ``true``.
``http-server.https.port``                              HTTPS server port.
``http-server.https.keystore.path``                     The location of the Java Keystore file that will be
                                                        used to secure TLS.
``http-server.https.keystore.key``                      The password for the keystore. This must match the
                                                        password you specified when creating the keystore.
======================================================= ======================================================

access-controls.properties
^^^^^^^^^^^^^^^^^^^^^^^^^^

Once a plugin that implements ``SystemAccessControl`` and
``SystemAccessControlFactory`` has been installed on the coordinator, it is
configured using an access-controls.properties file. All of the properties
other than access-control.name are specific to the ``SystemAccessControl``
plugin.

.. code-block:: none

  access-control.name=custom-access-control
  custom-property1=custom-value1
  custom-property2=custom-value2
  ...

The ``access-control.name`` property is used by Presto to find a registered
``SystemAccessControlFactory``. The remaining properties are passed as a map to
``SystemAccessControlFactory.create(Map<String, String> config)``.

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

Verify the password for a keystore file and view its contents using `keytool
<http://docs.oracle.com/javase/8/docs/technotes/tools/windows/keytool.html>`_.

.. code-block:: none

  $ keytool -list -v -k /etc/presto/presto.jks

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
