===========
JDBC Driver
===========

Presto can be accessed from Java using the JDBC driver.
Download :maven_download:`jdbc` and add it to the class path of your Java application.

The driver is also available from Maven Central:

.. parsed-literal::

    <dependency>
        <groupId>com.facebook.presto</groupId>
        <artifactId>presto-jdbc</artifactId>
        <version>\ |version|\ </version>
    </dependency>

Requirements
------------

The Presto JDBC driver has the following requirements:

* Java version 8 or higher.
* All users that connect to Presto with the JDBC driver must be granted access to
  query tables in the ``system.jdbc`` schema.

Connecting
----------

The following JDBC URL formats are supported:

.. code-block:: none

    jdbc:presto://host:port
    jdbc:presto://host:port/catalog
    jdbc:presto://host:port/catalog/schema

For example, use the following URL to connect to Presto
running on ``example.net`` port ``8080`` with the catalog ``hive``
and the schema ``sales``:

.. code-block:: none

    jdbc:presto://example.net:8080/hive/sales

The above URL can be used as follows to create a connection:

.. code-block:: java

    String url = "jdbc:presto://example.net:8080/hive/sales";
    Connection connection = DriverManager.getConnection(url, "test", null);

Connection Parameters
---------------------

The driver supports various parameters that may be set as URL parameters
or as properties passed to ``DriverManager``. Both of the following
examples are equivalent:

.. code-block:: java

    // properties
    String url = "jdbc:presto://example.net:8080/hive/sales";
    Properties properties = new Properties();
    properties.setProperty("user", "test");
    properties.setProperty("password", "secret");
    properties.setProperty("SSL", "true");
    Connection connection = DriverManager.getConnection(url, properties);

    // URL parameters
    String url = "jdbc:presto://example.net:8443/hive/sales?user=test&password=secret&SSL=true";
    Connection connection = DriverManager.getConnection(url);

These methods may be mixed; some parameters may be specified in the URL
while others are specified using properties. However, the same parameter
may not be specified using both methods.

Parameter Reference
-------------------

================================= =======================================================================
Name                              Description
================================= =======================================================================
``user``                          Username to use for authentication and authorization.
``password``                      Password to use for LDAP authentication.
``socksProxy``                    SOCKS proxy host and port. Example: ``localhost:1080``
``httpProxy``                     HTTP proxy host and port. Example: ``localhost:8888``
``protocols``                     Comma delineated list of HTTP protocols to use. Example: ``protocols=http11``.
                                  Acceptable values: ``http11,http10,http2``
``applicationNamePrefix``         Prefix to append to any specified ``ApplicationName`` client info
                                  property, which is used to set the source name for the Presto query.
                                  If neither this property nor ``ApplicationName`` are set, the source
                                  for the query will be ``presto-jdbc``.
``accessToken``                   Access token for token based authentication.
``timeZoneId``                    Timezone to be used for timestamp columns in query output.
                                  Example: ``timeZoneId=UTC``.
``SSL``                           Use HTTPS for connections
``SSLKeyStorePath``               The location of the Java KeyStore file that contains the certificate
                                  and private key to use for authentication.
``SSLKeyStorePassword``           The password for the KeyStore.
``SSLTrustStorePath``             The location of the Java TrustStore file that will be used
                                  to validate HTTPS server certificates.
``SSLTrustStorePassword``         The password for the TrustStore.
``KerberosRemoteServiceName``     Presto coordinator Kerberos service name. This parameter is
                                  required for Kerberos authentication.
``KerberosPrincipal``             The principal to use when authenticating to the Presto coordinator.
``KerberosUseCanonicalHostname``  Use the canonical hostname of the Presto coordinator for the Kerberos
                                  service principal by first resolving the hostname to an IP address
                                  and then doing a reverse DNS lookup for that IP address.
                                  This is enabled by default.
``KerberosConfigPath``            Kerberos configuration file.
``KerberosKeytabPath``            Kerberos keytab file.
``KerberosCredentialCachePath``   Kerberos credential cache.
``extraCredentials``              Extra credentials for connecting to external services. The
                                  extraCredentials is a list of key-value pairs. Example:
                                  ``foo:bar;abc:xyz`` will create credentials ``abc=xyz`` and ``foo=bar``
``customHeaders``                 Custom headers to inject through JDBC driver. The
                                  customHeaders is a list of key-value pairs. Example:
                                  ``testHeaderKey:testHeaderValue`` will inject the header ``testHeaderKey``
                                  with value ``testHeaderValue``. Values should be percent encoded.
================================= =======================================================================
