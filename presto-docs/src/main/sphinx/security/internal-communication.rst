=============================
Secure Internal Communication
=============================

The Presto cluster can be configured to use secured communication. Communication
between Presto nodes can be secured with SSL/TLS.

Internal SSL/TLS configuration
------------------------------

SSL/TLS is configured in the ``config.properties`` file.  The SSL/TLS on the
worker and coordinator nodes are configured using the same set of properties.
Every node in the cluster must be configured. Nodes that have not been
configured, or are configured incorrectly, will not be able to communicate with
other nodes in the cluster.

To enable SSL/TLS for Presto internal communication, do the following:

1. Disable HTTP endpoint.

    .. code-block:: none

        http-server.http.enabled=false

    .. warning::

        You can enable HTTPS while leaving HTTP enabled. In most cases this is a
        security hole. If you are certain you want to use this configuration, you
        should consider using an firewall to limit access to the HTTP endpoint to
        only those hosts that should be allowed to use it.

2. Configure the cluster to communicate using the fully qualified domain name (fqdn)
   of the cluster nodes. This can be done in either of the following ways:

   - If the DNS service is configured properly, we can just let the nodes to
     introduce themselves to the coordinator using the hostname taken from
     the system configuration (``hostname --fqdn``)

     .. code-block:: none

         node.internal-address-source=FQDN

   - It is also possible to specify each node's fully-qualified hostname manually.
     This will be different for every host. Hosts should be in the same domain to
     make it easy to create the correct SSL/TLS certificates.
     e.g.: ``coordinator.example.com``, ``worker1.example.com``, ``worker2.example.com``.

     .. code-block:: none

         node.internal-address=<node fqdn>


3. Generate a Java Keystore File. Every Presto node must be able to connect to
   any other node within the same cluster. It is possible to create unique
   certificates for every node using the fully-qualified hostname of each host,
   create a keystore that contains all the public keys for all of the hosts,
   and specify it for the client (see step #8 below). In most cases it will be
   simpler to use a wildcard in the certificate as shown below.

    .. code-block:: none

        keytool -genkeypair -alias example.com -keyalg RSA -keystore keystore.jks
        Enter keystore password:
        Re-enter new password:
        What is your first and last name?
          [Unknown]:  *.example.com
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
        Is CN=*.example.com, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown correct?
          [no]:  yes

        Enter key password for <presto>
                (RETURN if same as keystore password):

    .. Note: Replace `example.com` with the appropriate domain.

4. Distribute the Java Keystore File across the Presto cluster.

5. Enable the HTTPS endpoint.

    .. code-block:: none

        http-server.https.enabled=true
        http-server.https.port=<https port>
        http-server.https.keystore.path=<keystore path>
        http-server.https.keystore.key=<keystore password>

6. Change the discovery uri to HTTPS.

    .. code-block:: none

        discovery.uri=https://<coordinator fqdn>:<https port>

7. Configure the internal communication to require HTTPS.

    .. code-block:: none

        internal-communication.https.required=true

8. Configure the internal communication to use the Java keystore file.

    .. code-block:: none

        internal-communication.https.keystore.path=<keystore path>
        internal-communication.https.keystore.key=<keystore password>

Internal Authentication
-----------------------
Internal authentication can be enabled to authenticate all internal communication between nodes of the cluster.
It is

* Optional when configuring only `internal TLS encryption <#internal-ssl-tls-configuration>`_
  between nodes of the cluster
* Optional when configuring only :doc:`external authentication </security>` method
  between clients and the coordinator
* Mandatory when configuring both the above i.e internal TLS along with external authentication.

There are multiple ways to enable internal authentication described in below sections

1. JWT
~~~~~~

Enable JWT authentication to authenticate all communication between nodes of the cluster.
Enable JWT and set the shared secret to the same value in
:ref:`config.properties <config-properties>` on all nodes of the cluster using below configs:

.. code-block:: text

    internal-communication.jwt.enabled=true
    internal-communication.shared-secret=<secret>

For shared secret value, a large random key is recommended, and can be generated with the following Linux
command:

.. code-block:: text

    openssl rand 512 | base64

2. CERTIFICATE
~~~~~~~~~~~~~~

Setup a CERTIFICATE authentication method which is different than the external authentication method.

For example, if PASSWORD authentication is used between clients and coordinator, then
CERTIFICATE authentication can be used internally, by specifying this authentication method
in the same config as given below. The existing keystore configs setup in `internal ssl/tls configuration <#internal-ssl-tls-configuration>`_
will be used for certificate authentication.

.. code-block:: text

    http-server.authentication.type=PASSWORD,CERTIFICATE

3. KERBEROS
~~~~~~~~~~~

If :doc:`Kerberos</security/server>` authentication is enabled, specify valid Kerberos
credentials for the internal communication, in addition to the SSL/TLS properties.

    .. code-block:: none

        internal-communication.kerberos.enabled=true

.. note::

    The service name and keytab file used for internal Kerberos authentication is
    taken from server Kerberos authentication properties, documented in :doc:`Kerberos</security/server>`,
    ``http.server.authentication.krb5.service-name`` and ``http.server.authentication.krb5.keytab``
    respectively. Make sure you have the Kerberos setup done on the worker nodes as well.
    The Kerberos principal for internal communication is built from
    ``http.server.authentication.krb5.service-name`` after appending it with the hostname of
    the node where Presto is running on and default realm from Kerberos configuration.

Performance with SSL/TLS enabled
--------------------------------

Enabling encryption impacts performance. The performance degradation can vary
based on the environment, queries, and concurrency.

For queries that do not require transferring too much data between the Presto
nodes (e.g. ``SELECT count(*) FROM table``), the performance impact is negligible.

However, for CPU intensive queries which require a considerable amount of data
to be transferred between the nodes (for example, distributed joins, aggregations and
window functions, which require repartitioning), the performance impact might be
considerable. The slowdown may vary from 10% to even 100%+, depending on the network
traffic and the CPU utilization.

Advanced Performance Tuning
---------------------------

In some cases, changing the source of random numbers will improve performance
significantly.

By default, TLS encryption uses the ``/dev/urandom`` system device as a source of entropy.
This device has limited throughput, so on environments with high network bandwidth
(e.g. InfiniBand), it may become a bottleneck. In such situations, it is recommended to try
to switch the random number generator algorithm to ``SHA1PRNG``, by setting it via
``http-server.https.secure-random-algorithm`` property in ``config.properties`` on the coordinator
and all of the workers:

    .. code-block:: none

        http-server.https.secure-random-algorithm=SHA1PRNG

Be aware that this algorithm takes the initial seed from
the blocking ``/dev/random`` device. For environments that do not have enough entropy to seed
the ``SHAPRNG`` algorithm, the source can be changed to ``/dev/urandom``
by adding the ``java.security.egd`` property to ``jvm.config``:

    .. code-block:: none

        -Djava.security.egd=file:/dev/urandom
