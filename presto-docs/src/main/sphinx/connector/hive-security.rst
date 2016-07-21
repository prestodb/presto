===========================
Hive Security Configuration
===========================

Authorization
=============

You can enable authorization checks for the :doc:`/connector/hive` by setting
the ``hive.security`` property in the Hive catalog properties file. This
property must be one of the following values:

================================================== ============================================================
Property Value                                     Description
================================================== ============================================================
``allow-all`` (default value)                      No authorization checks are enforced, thus allowing any
                                                   operation.

``read-only``                                      Operations that read data or metadata, such as ``SELECT``,
                                                   are permitted, but none of the operations that write data or
                                                   metadata, such as ``CREATE``, ``INSERT`` or ``DELETE``, are
                                                   allowed.

``sql-standard``                                   Users are permitted to perform the operations as long as
                                                   they have the required privileges as per the SQL standard.
                                                   In this mode, Presto enforces the authorization checks for
                                                   queries based on the privileges defined in Hive metastore.
                                                   To alter these privileges, use the :doc:`/sql/grant` and
                                                   :doc:`/sql/revoke` commands.
================================================== ============================================================

Authentication
==============

The default security configuration of the :doc:`/connector/hive` does not use
authentication when connecting to a Hadoop cluster. All queries are executed as
the user who runs the Presto process, regardless of which user submits the
query.

The Hive connector provides additional security options to support Hadoop
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

.. note::

    If your ``krb5.conf`` location is different from ``/etc/krb5.conf`` you
    must set it explicitly using the ``java.security.krb5.conf`` JVM property
    in ``jvm.config`` file.

    Example: ``-Djava.security.krb5.conf=/example/path/krb5.conf``.

Hive Metastore Thrift Service Authentication
--------------------------------------------

In a Kerberized Hadoop cluster, Presto connects to the Hive metastore Thrift
service using :abbr:`SASL (Simple Authentication and Security Layer)` and
authenticates using Kerberos. Kerberos authentication for the metastore is
configured in the connector's properties file using the following properties:

================================================== ============================================================
Property Name                                      Description
================================================== ============================================================
``hive.metastore.authentication.type``             Hive metastore authentication type.

``hive.metastore.service.principal``               The Kerberos principal of the Hive metastore service.

``hive.metastore.client.principal``                The Kerberos principal that Presto will use when connecting
                                                   to the Hive metastore service.

``hive.metastore.client.keytab``                   Hive metastore client keytab location.
================================================== ============================================================

``hive.metastore.authentication.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

One of ``NONE`` or ``KERBEROS``. When using the default value of ``NONE``,
Kerberos authentication is disabled and no other properties need to be
configured.

When set to ``KERBEROS`` the Hive connector will connect to the Hive metastore
Thrift service using SASL and authenticate using Kerberos.

This property is optional; the default is ``NONE``.

``hive.metastore.service.principal``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Kerberos principal of the Hive metastore service. The Presto coordinator
will use this to authenticate the Hive metastore.

The ``_HOST`` placeholder can be used in this property value. When connecting
to the Hive metastore, the Hive connector will substitute in the hostname of
the **metastore** server it is connecting to. This is useful if the metastore
runs on multiple hosts.

Example: ``hive/hive-server-host@EXAMPLE.COM`` or ``hive/_HOST@EXAMPLE.COM``.

This property is optional; no default value.

``hive.metastore.client.principal``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Kerberos principal that Presto will use when connecting to the Hive
metastore.

The ``_HOST`` placeholder can be used in this property value. When connecting
to the Hive metastore, the Hive connector will substitute in the hostname of
the **worker** node Presto is running on. This is useful if each worker node
has its own Kerberos principal.

Example: ``presto/presto-server-node@EXAMPLE.COM`` or
``presto/_HOST@EXAMPLE.COM``.

This property is optional; no default value.

.. warning::

    The principal specified by ``hive.metastore.client.principal`` must have
    sufficient privileges to remove files and directories within the
    ``hive/warehouse`` directory. If the principal does not, only the metadata
    will be removed, and the data will continue to consume disk space.

    This occurs because the Hive metastore is responsible for deleting the
    internal table data. When the metastore is configured to use Kerberos
    authentication, all of the HDFS operations performed by the metastore are
    impersonated. Errors deleting data are silently ignored.

``hive.metastore.client.keytab``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the keytab file that contains a key for the principal specified by
``hive.metastore.client.principal``. This file must be readable by the
operating system user running Presto.

This property is optional; no default value.

Example configuration with ``NONE`` authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.metastore.authentication.type=NONE

The default authentication type for the Hive metastore is ``NONE``. When the
authentication type is ``NONE``, Presto connects to an unsecured Hive
metastore. Kerberos is not used.

Example configuration with ``KERBEROS`` authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.metastore.authentication.type=KERBEROS
    hive.metastore.principal=hive/hive-metastore-host.example.com@EXAMPLE.COM
    hive.metastore.client.principal=presto@EXAMPLE.COM
    hive.metastore.client.keytab=/etc/presto/hive.keytab

When the authentication type for the Hive metastore Thrift service is
``KERBEROS``, Presto will connect as the Kerberos principal specified by the
property ``hive.metastore.client.principal``. Presto will authenticate this
principal using the keytab specified by the ``hive.metastore.client.keytab``
property, and will verify that the identity of the metastore matches
``hive.metastore.service.principal``.

Keytab files must be distributed to every node in the cluster that runs Presto.

:ref:`Additional Information About Keytab Files.<hive-security-additional-keytab>`

HDFS Authentication
-------------------

In a Kerberized Hadoop cluster, Presto authenticates to HDFS using Kerberos.
Kerberos authentication for HDFS is configured in the connector's properties
file using the following properties:

================================================== ============================================================
Property Name                                      Description
================================================== ============================================================
``hive.hdfs.authentication.type``                  HDFS authentication type.
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.hdfs.impersonation.enabled``                Enable HDFS end-user impersonation.

``hive.hdfs.presto.principal``                     The Kerberos principal that Presto will use when connecting
                                                   to HDFS.

``hive.hdfs.presto.keytab``                        HDFS client keytab location.
================================================== ============================================================

``hive.hdfs.authentication.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

One of ``NONE`` or ``KERBEROS``. When using the default value of ``NONE``,
Kerberos authentication is disabled and no other properties need to be
configured.

When set to ``KERBEROS``, the Hive connector authenticates to HDFS using
Kerberos.

This property is optional; the default is ``NONE``.

``hive.hdfs.impersonation.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Enable end-user HDFS impersonation.

The section :ref:`End User Impersonation<hive-security-impersonation>` gives an
in-depth explanation of HDFS impersonation.

This property is optional; the default is ``false``.

``hive.hdfs.presto.principal``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Kerberos principal that Presto will use when connecting to HDFS.

The ``_HOST`` placeholder can be used in this property value. When connecting
to HDFS, the Hive connector will substitute in the hostname of the **worker**
node Presto is running on. This is useful if each worker node has its own
Kerberos principal.

Example: ``presto-hdfs-superuser/presto-server-node@EXAMPLE.COM`` or
``presto-hdfs-superuser/_HOST@EXAMPLE.COM``.

This property is optional; no default value.

``hive.hdfs.presto.keytab``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the keytab file that contains a key for the principal specified by
``hive.hdfs.presto.principal``. This file must be readable by the operating
system user running Presto.

This property is optional; no default value.

.. _hive-security-simple:

Example configuration with ``NONE`` authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=NONE

The default authentication type for HDFS is ``NONE``. When the authentication
type is ``NONE``, Presto connects to HDFS using Hadoop's simple authentication
mechanism. Kerberos is not used.

.. _hive-security-kerberos:

Example configuration with ``KERBEROS`` authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=KERBEROS
    hive.hdfs.presto.principal=hdfs@EXAMPLE.COM
    hive.hdfs.presto.keytab=/etc/presto/hdfs.keytab

When the authentication type is ``KERBEROS``, Presto accesses HDFS as the
principal specified by the ``hive.hdfs.presto.principal`` property. Presto will
authenticate this principal using the keytab specified by the
``hive.hdfs.presto.keytab`` keytab.

Keytab files must be distributed to every node in the cluster that runs Presto.

:ref:`Additional Information About Keytab Files.<hive-security-additional-keytab>`

.. _hive-security-impersonation:

End User Impersonation
======================

Impersonation Accessing HDFS
----------------------------

Presto can impersonate the end user who is running a query. In the case of a
user running a query from the command line interface, the end user is the
username associated with the Presto CLI process or argument to the optional
``--user`` option. Impersonating the end user can provide additional security
when accessing HDFS if HDFS permissions or ACLs are used.

HDFS Permissions and ACLs are explained in the `HDFS Permissions Guide
<https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html>`_.

.. _hive-security-simple-impersonation:

``NONE`` authentication with HDFS impersonation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=NONE
    hive.hdfs.impersonation=true

When using ``NONE`` authentication with impersonation, Presto impersonates
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

:ref:`Additional Information About Keytab Files.<hive-security-additional-keytab>`

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
nodes. This means that the keytab needs to be in the same location on every
node.

You should ensure that the keytab files have the correct permissions on every
node after distributing them.
