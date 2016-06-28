===========================
Hive Security Configuration
===========================

The default security configuration of the :doc:`/connector/hive` uses
``simple`` authentication to connect to a Hadoop cluster. All queries are
executed as the user who runs the Presto process, regardless of the user who
submits the query.

The Hive connector provides additonal security options to support Hadoop
clusters that have been configured to use :ref:`Kerberos
<hive-security-kerberos-support>`.

When accessing :abbr:`HDFS (Hadoop Distributed File System)`, Presto can
:ref:`impersonate<hive-security-impersonation>` the end user who is running the
query. This can be used with HDFS permissions and :abbr:`ACLs (Access Control
Lists)` to provide additional security for data.

.. _hive-security-kerberos-support:

Kerberos Support
================

In order to use the Hive connector with a Hadoop cluster that uses ``kerberos``
authentication, you will need to configure the connector to work with two
services on the Hadoop cluster:

* The Hive metastore Thrift service
* The Hadoop Distributed File System (HDFS)

Access to these services by the Hive connector is configured in the properties
file that contains the general Hive connector configuration.

Hive Metastore Thrift Service Authentication
--------------------------------------------

In a Kerberized Hadoop cluster, Presto accesses the Hive metastore Thrift
service through :abbr:`SASL (Simple Authentication and Security Layer)`.
Kerberos authentication for the metastore is configured in the connector's
properties file using the following properties:

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.metastore.authentication.type``             One of ``SIMPLE`` or ``SASL``.  When using the default value ``SIMPLE`` 
                                                   of ``SIMPLE``, Kerberos authentication is disabled and no
                                                   other properties need to be configured.

                                                   When set to ``SASL`` the Hive connector will connect to the
                                                   Hive metastore Thrift service using SASL and authenticate
                                                   with Kerberos.

``hive.metastore.principal``                       The Kerberos principal of the Hive metastore. The Presto
                                                   coordinator will use this to authenticate the Hive
                                                   metastore.

``hive.metastore.presto.principal``                The Kerberos principal that Presto will use when connecting
                                                   to the Hive metastore.

``hive.metastore.presto.keytab``                   The path to the keytab file that contains a key for the
                                                   principal specified by ``hive.metastore.presto.principal``.

================================================== ============================================================ ==========

``SIMPLE``
^^^^^^^^^^

.. code-block:: none

    hive.metastore.authentication.type=SIMPLE

``SIMPLE`` authentication is the default authentication mechanism for the Hive
metastore. When using ``SIMPLE`` authentication, Presto connects to an
unsecured Hive metastore. Kerberos is not used.

``SASL``
^^^^^^^^

.. code-block:: none

    hive.metastore.authentication.type=SASL
    hive.metastore.principal=hive/hive-metastore-host.example.com@EXAMPLE.COM
    hive.metastore.presto.principal=presto@EXAMPLE.COM
    hive.metastore.presto.keytab=/etc/presto/hive.keytab

When using ``SASL`` authentication for the Hive metastore Thrift service,
Presto will connect as the Kerberos principal specified by the property
``hive.metastore.presto.principal``.  Presto will authenticate this principal
using the keytab specified by the ``hive.metastore.presto.keytab`` property,
and will verify that the identity of the metastore matches
``hive.metastore.principal``.

Keytab files must be distributed to every node in the cluster that runs Presto.

:ref:`Additional information on keytab files.<hive-security-additional-keytab>`

HDFS Authentication
-------------------

In a Kerberized Hadoop cluster, Presto authenticates to HDFS using Kerberos.
Kerberos authentication for HDFS is configured in the connector's properties
file using the following properties:

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.hdfs.authentication.type``                  One of ``SIMPLE`` or ``KERBEROS``.  When using the default   ``SIMPLE``
                                                   value of ``SIMPLE``, Kerberos authentication is disabled and
                                                   no other properties need to be configured.

                                                   When set to ``KERBEROS``, the Hive connector authenticates
                                                   to HDFS using Kerberos.

``hive.hdfs.presto.principal``                     The Kerberos principal that Presto will use when connecting
                                                   to HDFS.

``hive.hdfs.presto.keytab``                        The path to the keytab file that contains a key for the
                                                   principal specified by ``hive.hdfs.presto.principal``.

================================================== ============================================================ ==========

.. _hive-security-simple:

``SIMPLE``
^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=SIMPLE

``SIMPLE`` authentication is the default authentication mechanism for HDFS.
When using ``SIMPLE`` authentication, Presto connects to HDFS using Hadoop's
simple authentication mechanism. Kerberos is not used.

.. _hive-security-kerberos:

``KERBEROS``
^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=KERBEROS
    hive.hdfs.presto.principal=hdfs@EXAMPLE.COM
    hive.hdfs.presto.keytab=/etc/presto/hdfs.keytab

When using ``KERBEROS`` authentication, Presto accesses HDFS as the principal
specified by the ``hive.hdfs.presto.principal`` property. Presto will
authenticate this principal using the keytab specified by the
``hive.hdfs.presto.keytab`` keytab.

Keytab files must be distributed to every node in the cluster that runs Presto.

:ref:`Additional information on keytab files.<hive-security-additional-keytab>`

.. _hive-security-impersonation:

End User Impersonation
======================

Impersonation Accessing HDFS
----------------------------

Presto can impersonate the end user who is running a query. In the case of a
user running a query from the command line interface, the end user is the
username associated with the Presto cli process or argument to the optional
``--user`` option.  Impersonating the end user can provide additional security
when accessing HDFS if HDFS permissions or ACLs are used.

HDFS Permissions and ACLs are explained in the `HDFS Permissions Guide
<https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html>`_.

.. _hive-security-simple-impersonation:

``SIMPLE`` authentication with HDFS impersonation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=SIMPLE
    hive.hdfs.impersonation=true

When using ``SIMPLE`` authentication with impersonation, Presto impersonates
the user who is running the query when accessing HDFS. The user Presto is
running as must be allowed to impersonate this user, as discussed in the
section :ref:`configuring-hadoop-impersonation`. Kerberos is not used.

.. _hive-security-kerberos-impersonation:

``KERBEROS`` Authentication With HDFS Impersonation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=KERBEROS
    hive.hdfs.impersonation=true
    hive.hdfs.presto.principal=presto@EXAMPLE.COM
    hive.hdfs.presto.keytab=/etc/presto/hdfs.keytab

When using ``KERBEROS`` authentication with impersonation, Presto impersonates
the user who is running the query when accessing HDFS. The principal
specified by the ``hive.hdfs.presto.principal`` property must be allowed to
impersonate this user, as discussed in the section
:ref:`configuring-hadoop-impersonation`. Presto authenticates
``hive.hdfs.presto.principal`` using the keytab specified by
``hive.hdfs.presto.keytab``.

Keytab files must be distributed to every node in the cluster that runs Presto.

:ref:`Additional information on keytab files.<hive-security-additional-keytab>`

Impersonation Accessing the Hive Metastore
------------------------------------------

Presto does not currently support impersonating the end user when accessing the
Hive metastore.

.. _configuring-hadoop-impersonation:

Impersonation in Hadoop
-----------------------

In order to use :ref:`hive-security-simple-impersonation` or
:ref:`hive-security-kerberos-impersonation`, the Hadoop cluster must be
configured to allow the user or principal that Presto is running as to
impersonate the users who log in to Presto. Impersonation in Hadoop is
configured in the file :file:`core-site.xml`. A complete description of the
configuration options can be found in the `Hadoop documentation
<https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html#Configurations>`_.

.. _hive-security-additional-keytab:

Additional Information About Keytab Files
=========================================

Keytab files contain encryption keys that are used to authenticate principals
to the Kerberos :abbr:`KDC (Key Distribution Center)`. These encryption keys
must be stored securely; you should take the same precautions to protect them
that you would to protect ssh private keys.

In particular, access to keytab files should be limited to the accounts that
actually need to use them to authenticate. In practice, this is the user that
the Presto process runs as. The ownership and permissions on keytab files
should be set to prevent other users from reading or modifying the files.

Keytab files need to be distributed to every node running Presto. Under common
deployment situations, the Hive connector configuration will be the same on all
nodes.  This means that the keytab needs to be in the same location on every
node.

You should ensure that the keytab files have the correct permissions on every
node after distributing them.
