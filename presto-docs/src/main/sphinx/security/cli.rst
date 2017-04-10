===========================
CLI Kerberos Authentication
===========================

The Presto :doc:`/installation/cli` can connect to a :doc:`Presto coordinator
</security/server>` that has Kerberos authentication enabled.

Environment Configuration
-------------------------

.. |subject_node| replace:: client

.. include:: kerberos-services.fragment
.. include:: kerberos-configuration.fragment

Kerberos Principals and Keytab Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each user who connects to the Presto coordinator needs a Kerberos principal.
You will need to create these users in Kerberos using `kadmin
<http://web.mit.edu/kerberos/krb5-latest/doc/admin/admin_commands/kadmin_local.html>`_.

Additionally, each user needs a `keytab file
<http://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html>`_. The
keytab file can be created using :command:`kadmin` after you create the
principal.

.. code-block:: none

    kadmin
    > addprinc -randkey someuser@EXAMPLE.COM
    > ktadd -k /home/someuser/someuser.keytab someuser@EXAMPLE.COM

.. include:: ktadd-note.fragment

.. include:: jce-policy.fragment

Java Keystore File for TLS
^^^^^^^^^^^^^^^^^^^^^^^^^^

Access to the Presto coordinator must be through https when using Kerberos
authentication. The Presto coordinator uses a :ref:`Java Keystore
<server_java_keystore>` file for its TLS configuration. This file can be
copied to the client machine and used for its configuration.

Presto CLI execution
--------------------

In addition to the options that are required when connecting to a Presto
coordinator that does not require Kerberos authentication, invoking the CLI
with Kerberos support enabled requires a number of additional command line
options. The simplest way to invoke the CLI is with a wrapper script.

.. code-block:: none

    #!/bin/bash

    ./presto \
      --server https://presto-coordinator.example.com:7778 \
      --enable-authentication \
      --krb5-config-path /etc/krb5.conf \
      --krb5-principal someuser@EXAMPLE.COM \
      --krb5-keytab-path /home/someuser/someuser.keytab \
      --krb5-remote-service-name presto \
      --keystore-path /tmp/presto.jks \
      --keystore-password password \
      --catalog <catalog> \
      --schema <schema>

=============================== =========================================================================
Option                          Description
=============================== =========================================================================
``--server``                    The address and port of the Presto coordinator.  The port must
                                be set to the port the Presto coordinator is listening for HTTPS
                                connections on.
``--enable-authentication``     Enables Kerberos authentication.
``--krb5-config-path``          Kerberos configuration file.
``--krb5-principal``            The principal to use when authenticating to the coordinator.
``--krb5-keytab-path``          The location of the the keytab that can be used to
                                authenticate the principal specified by ``--krb5-principal``
``--krb5-remote-service-name``  Presto coordinator Kerberos service name.
``--keystore-path``             The location of the Java Keystore file that will be used
                                to secure TLS.
``--keystore-password``         The password for the keystore. This must match the
                                password you specified when creating the keystore.
=============================== =========================================================================

Troubleshooting
---------------

Many of the same steps that can be used when troubleshooting the :ref:`Presto
coordinator <coordinator-troubleshooting>` apply to troubleshooting the CLI.

Additional Kerberos Debugging Information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can enable additional Kerberos debugging information for the Presto CLI
process by passing ``-Dsun.security.krb5.debug=true`` as a JVM argument when
starting the CLI process. Doing so requires invoking the CLI JAR via ``java``
instead of running the self-executable JAR directly. The self-executable jar
file cannot pass the option to the JVM.

.. code-block:: none

    #!/bin/bash

    java \
      -Dsun.security.krb5.debug=true \
      -jar presto-cli-*-executable.jar \
      --server https://presto-coordinator.example.com:7778 \
      --enable-authentication \
      --krb5-config-path /etc/krb5.conf \
      --krb5-principal someuser@EXAMPLE.COM \
      --krb5-keytab-path /home/someuser/someuser.keytab \
      --krb5-remote-service-name presto \
      --keystore-path /tmp/presto.jks \
      --keystore-password password \
      --catalog <catalog> \
      --schema <schema>

The :ref:`additional resources <server_additional_resources>` listed in the
documentation for setting up Kerberos authentication for the Presto coordinator
may be of help when interpreting the Kerberos debugging messages.
