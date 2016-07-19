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


Hive Data Stored in Amazon S3
=============================

The Hive Connector can read data from files stored in S3, assuming the Hive
Metastore has tables defined as "external" which have locations defined as
S3 URIs.

Presto registers its own S3 filesystem for the following URI schemes:
``s3://``, ``s3a://``, ``s3n://`` and it registers the default Hadoop S3
FileSystem (``org.apache.hadoop.fs.s3.S3FileSystem``) for ``s3bfs://`` URIs.
The following assumes your are using the native Presto S3 FileSystem implementation.

Configuration Properties
------------------------

============================================ ====================================================== ==========
Property Name                                Description                                            Default
============================================ ====================================================== ==========
``presto.s3.access-key``                     Default API access key to use

``presto.s3.secret-key``                     Default API secret key to use

``presto.s3.staging-directory``              Local staging directory for data written to S3         ``java.io.tmpdir``

``presto.s3.use-instance-credentials``       Use the EC2 metadata service to retrieve API           ``true``
                                             credentials.  This works with IAM roles in EC2.

``presto.s3.pin-client-to-current-region``   Pin S3 requests to the same region as the EC2 instance ``false``
                                             where Presto is running.

``presto.s3.ssl.enabled``                    Use HTTPS to communicate with the S3 API               ``true``

``presto.s3.sse.enabled``                    Use S3 server-side encryption                          ``false``

``presto.s3.kms-key-id``                     If set, use S3 client-side encryption and use the AWS
                                             KMS to store encryption keys and use the value of
                                             this property as the KMS Key ID for newly created
                                             objects.

``presto.s3.encryption-materials-provider``  If set, use S3 client-side encryption and use the
                                             value of this property as the fully qualified name of
                                             a java class which implements the S3 Java SDK's
                                             ``EncryptionMaterialsProvider`` interface.   If the
                                             class also implements ``Configurable`` from the Hadoop
                                             API, the Hadoop configuration will be passed in after
                                             the object has been created.
============================================ ====================================================== ==========

If you are running Presto on Amazon EC2 using EMR or another facility, it is highly recommended that you
set ``presto.s3.use-instance-credentials`` to ``true`` and use IAM Roles for EC2 to govern access to S3.
If this is the case, your EC2 instances will need to be assigned an IAM Role which grants appropriate
access to the data stored in the S3 bucket(s) you wish to use.  This is much cleaner than setting AWS
access and secret keys in the ``presto.s3.access-key`` and ``presto.s3.secret-key`` settings, and also
allows EC2 to automatically rotate credentials on a regular basis without any additional work on your part.


Tuning Properties
-----------------

The following tuning properties affect how many retries are attempted when communicating with S3, etc.
Most of these parameters affect settings on the ``ClientConfiguration`` object associated with
the ``AmazonS3Client``.

============================================ ====================================================== ==========
Property Name                                Description                                            Default
============================================ ====================================================== ==========
``presto.s3.max-error-retries``              Max number of error retries, set on the S3 client      ``10``

``presto.s3.max-client-retries``             Max number of read attempts to retry                   ``3``

``presto.s3.max-backoff-time``               Use exponential backoff starting at 1 second up to     ``10 minutes``
                                             this maximum value when communicating with S3.

``presto.s3.max-retry-time``                 Maximum time to retry communicating with S3.           ``10 minutes``

``presto.s3.connect-timeout``                TCP connection timeout                                 ``5 seconds``

``presto.s3.socket-timeout``                 TCP socket read timeout                                ``5 seconds``

``presto.s3.max-connections``                Max number of simultaneous open connections to S3      ``500``

``presto.s3.multipart.min-file-size``        Min file size before multi-part upload to S3 is used   ``16 MB``

``presto.s3.multipart.min-part-size``        Multi-part upload part size                            ``5 MB``
============================================ ====================================================== ==========

S3 Data Encryption
------------------

Presto supports reading and writing encrypted data in S3 using both server-side encryption with
S3 managed keys and client-side encryption using either the Amazon KMS or a software plugin to
manage AES encryption keys.

With `S3 server-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html>`_,
(known as "SSE-S3" in the Amazon documentation) the S3 infrastructure takes care of all encryption and decryption
work (with the exception of SSL to the client, assuming you have ``presto.s3.ssl.enabled`` set to ``true``).
S3 also manages all the encryption keys for you.  To enable this, set ``presto.s3.sse.enabled`` to ``true``.

With `S3 client-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html>`_,
S3 stores encrypted data and the encryption keys are managed outside of the S3 infrastructure.  Data is encrypted
and decrypted by Presto instead of in the S3 infrastructure. In this case, encryption keys can either by
managed using the AWS KMS or your own key management system. To use the AWS KMS for key management, set
``presto.s3.kms-key-id`` to the UUID of a KMS Key.  Your AWS credentials or EC2 IAM Role will need to be
granted permission to use the given key as well.

To use a custom encryption key management system, set ``presto.s3.encryption-materials-provider`` to the
fully qualified name of a class which implements the
`EncryptionMaterialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/EncryptionMaterialsProvider.html>`_
interface from the AWS Java SDK.  This class will have to be accessible to the Hive Connector through the
classpath and must be able to communicate with your custom key management system.  If this class also implements
the ``org.apache.hadoop.conf.Configurable`` interface from the Hadoop Java API, then the Hadoop configuration
will be passed in after the object instance is created and before it is asked to provision or retrieve any
encryption keys.

