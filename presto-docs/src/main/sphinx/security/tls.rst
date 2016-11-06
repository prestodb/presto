==============================
Java Keystores and Truststores
==============================

.. _server_java_keystore:

Java Keystore File for TLS
--------------------------

Access to the Presto coordinator must be through HTTPS when using Kerberos
authentication. The Presto coordinator uses a :ref:`Java Keystore
<server_java_keystore>` file for its TLS configuration. These keys are
generated using :command:`keytool` and stored in a Java Keystore file for the
Presto coordinator.

The alias in the :command:`keytool` command line should match the principal that the
Presto coordinator will use.

You'll be prompted for the first and last name. Use the Common Name that will
be used in the certificate. In this case, it should be the unqualified hostname
of the Presto coordinator. In the following example, you can see this in the prompt
that confirms the information is correct:

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

Troubleshooting
---------------

.. _troubleshooting_keystore:

Java Keystore File Verification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Verify the password for a keystore file and view its contents using `keytool
<http://docs.oracle.com/javase/8/docs/technotes/tools/windows/keytool.html>`_.

.. code-block:: none

    $ keytool -list -v -k /etc/presto/presto.jks
