===========================
Hive Security Configuration
===========================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Authorization
=============

You can enable authorization checks for the :doc:`/connector/hive` by setting
the ``hive.security`` property in the Hive catalog properties file. This
property must be one of the following values:

================================================== ============================================================
Property Value                                     Description
================================================== ============================================================
``legacy`` (default value)                         Few authorization checks are enforced, thus allowing most
                                                   operations. The config properties ``hive.allow-drop-table``,
                                                   ``hive.allow-rename-table``, ``hive.allow-add-column``,
                                                   ``hive.allow-drop-column`` and
                                                   ``hive.allow-rename-column`` are used.

``read-only``                                      Operations that read data or metadata, such as ``SELECT``,
                                                   are permitted, but none of the operations that write data or
                                                   metadata, such as ``CREATE``, ``INSERT`` or ``DELETE``, are
                                                   allowed.

``file``                                           Authorization checks are enforced using a config file specified
                                                   by the Hive configuration property ``security.config-file``.
                                                   See :ref:`hive-file-based-authorization` for details.

``sql-standard``                                   Users are permitted to perform the operations as long as
                                                   they have the required privileges as per the SQL standard.
                                                   In this mode, Presto enforces the authorization checks for
                                                   queries based on the privileges defined in Hive metastore.
                                                   To alter these privileges, use the :doc:`/sql/grant` and
                                                   :doc:`/sql/revoke` commands.
                                                   See :ref:`hive-sql-standard-based-authorization` for details.

``ranger``                                         Users are permitted to perform the operations as per the
                                                   authorization policies configured in Ranger Hive service.
                                                   See :ref:`hive-ranger-based-authorization` for details.
================================================== ============================================================

.. _hive-sql-standard-based-authorization:

SQL Standard Based Authorization
--------------------------------

When ``sql-standard`` security is enabled, Presto enforces the same SQL
standard based authorization as Hive does.

Since Presto's ``ROLE`` syntax support matches the SQL standard, and
Hive does not exactly follow the SQL standard, there are the following
limitations and differences:

* ``CREATE ROLE role WITH ADMIN`` is not supported.
* The ``admin`` role must be enabled to execute ``CREATE ROLE`` or ``DROP ROLE``.
* ``GRANT role TO user GRANTED BY someone`` is not supported.
* ``REVOKE role FROM user GRANTED BY someone`` is not supported.
* By default, all a user's roles except ``admin`` are enabled in a new user session.
* One particular role can be selected by executing ``SET ROLE role``.
* ``SET ROLE ALL`` enables all of a user's roles except ``admin``.
* The ``admin`` role must be enabled explicitly by executing ``SET ROLE admin``.

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

.. warning::

  Access to the Presto coordinator should be secured using Kerberos when using
  Kerberos authentication to Hadoop services. Failure to secure access to the
  Presto coordinator could result in unauthorized access to sensitive data on
  the Hadoop cluster.

  See :doc:`/security/server` and :doc:`/security/cli`
  for information on setting up Kerberos authentication.

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
``hive.metastore-impersonation-enabled``           Enable metastore end-user impersonation.
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
    hive.metastore.service.principal=hive/hive-metastore-host.example.com@EXAMPLE.COM
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
    hive.hdfs.impersonation.enabled=true

When using ``NONE`` authentication with impersonation, Presto impersonates
the user who is running the query when accessing HDFS. The user Presto is
running as must be allowed to impersonate this user, as discussed in the
section :ref:`configuring-hadoop-impersonation`. Kerberos is not used.

.. _hive-security-kerberos-impersonation:

``KERBEROS`` Authentication With HDFS Impersonation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=KERBEROS
    hive.hdfs.impersonation.enabled=true
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

.. _hive-file-based-authorization:

File Based Authorization
========================

The config file is specified using JSON and is composed of three sections,
each of which is a list of rules that are matched in the order specified
in the config file. The user is granted the privileges from the first
matching rule. All regexes default to ``.*`` if not specified.

Schema Rules
------------

These rules govern who is considered an owner of a schema.

* ``user`` (optional): regex to match against user name.

* ``schema`` (optional): regex to match against schema name.

* ``owner`` (required): boolean indicating ownership.

Table Rules
-----------

These rules govern the privileges granted on specific tables.

* ``user`` (optional): regex to match against user name.

* ``schema`` (optional): regex to match against schema name.

* ``table`` (optional): regex to match against table name.

* ``privileges`` (required): zero or more of ``SELECT``, ``INSERT``,
  ``DELETE``, ``OWNERSHIP``, ``GRANT_SELECT``.

Session Property Rules
----------------------

These rules govern who may set session properties.

* ``user`` (optional): regex to match against user name.

* ``property`` (optional): regex to match against session property name.

* ``allowed`` (required): boolean indicating whether this session property may be set.

See below for an example.

.. code-block:: json

    {
      "schemas": [
        {
          "user": "admin",
          "schema": ".*",
          "owner": true
        },
        {
          "user": "guest",
          "owner": false
        },
        {
          "schema": "default",
          "owner": true
        }
      ],
      "tables": [
        {
          "user": "admin",
          "privileges": ["SELECT", "INSERT", "DELETE", "OWNERSHIP"]
        },
        {
          "user": "banned_user",
          "privileges": []
        },
        {
          "schema": "default",
          "table": ".*",
          "privileges": ["SELECT"]
        }
      ],
      "sessionProperties": [
        {
          "property": "force_local_scheduling",
          "allow": true
        },
        {
          "user": "admin",
          "property": "max_split_size",
          "allow": true
        }
      ]
    }

.. _hive-ranger-based-authorization:

Ranger Based Authorization
==========================

Apache Ranger is a widely used framework for providing centralized security
administration and management.
Ranger supports various components plugin to allow authorization policy
management and verification by integrating with components.
Ranger Hive plugin is used to extend authorization for Hive clients such as
Beeline.
Presto ranger plugin for Hive connector can be integrated with Ranger
as a access control system to perform authorization for presto hive connector
queries configure with polices defined Ranger Hive component . When a query is
submitted to Presto, Presto parses and analyzes the query to understand the
privileges required by the user to access objects such as schemas and tables.
Once a list of these objects is created, Presto communicates with the Ranger
service to determine if the request is valid. If the request is valid, the
query continues to execute. If the request is invalid, because the user does
not have the necessary privileges to query an object, an error is returned.
Ranger policies are cached in Presto to improve performance.

Authentication is handled outside of Ranger, for example using LDAP, and
Ranger uses the authenticated user and user groups to associate with the
policy definition.

Requirements
------------

Before you configure Presto for any integration with Apache Ranger,
verify the following prerequisites:

Presto coordinator and workers have the appropriate network access to
communicate with the Ranger service. Typically this is port 6080.

Apache Ranger 2.1.0 or higher must be used

Policies
--------

A policy is a combination of set of resources and the associated privileges.
Ranger provides a user interface, or optionally a REST API, to create
and manage these access control policies.

Users, groups, and roles
------------------------

Apache Ranger has UserGroups sync mechanism by which Users, groups, and
roles are sourced from your configured authentication system with Apache
Ranger.

Supported authorizations
------------------------

Ranger Hive service allows to configure privileges at schema, table, column
level. Note to restrict access to specific user and groups ranger policies
needs to configure with explict deny conditions.

Access for listing schema, show tables metadata & configuring session
properties are enabled by default.

Configuration properties
------------------------

================================================== ============================================================ ============
Property Name                                      Description                                                  Default
================================================== ============================================================ ============
``hive.ranger.rest-endpoint``                      URL address of the Ranger REST service. Kerberos
                                                   authentication is not supported yet.

``hive.ranger.refresh-policy-period``              Interval at which cached policies are refreshed              60s

``hive.ranger.policy.hive-servicename``            Ranger Hive plugin service name

``hive.ranger.service.basic-auth-username``        Ranger Hive plugin username configured with
                                                   for Basic HTTP auth.

``hive.ranger.service.basic-auth-password``        Ranger Hive plugin password configured with
                                                   for Basic HTTP auth.

``hive.ranger.audit.path``                         Ranger Audit configuration - ranger audit file path

``ranger.http-client.key-store-path``              Ranger SSL configuration - client keystore file path

``ranger.http-client.key-store-password``          Ranger SSL configuration - client keystore password

``ranger.http-client.trust-store-path``            Ranger SSL configuration - client trust-store file path

``ranger.http-client.trust-store-password``        Ranger SSL configuration - client trust-store password

================================================== ============================================================ ============

HDFS wire encryption
--------------------

In a Kerberized Hadoop cluster with enabled HDFS wire encryption you can enable
Presto to access HDFS by using below property.

===================================== ==========================================
Property Name                         Description
===================================== ==========================================
``hive.hdfs.wire-encryption.enabled`` Enables HDFS wire encryption.
                                      Possible values are ``true`` or ``false``.
===================================== ==========================================

.. note::

    Depending on Presto installation configuration, using wire encryption may
    impact query execution performance.
