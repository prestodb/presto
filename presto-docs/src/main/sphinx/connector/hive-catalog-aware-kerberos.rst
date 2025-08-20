==========================================
Hive Catalog-Aware Kerberos Authentication
==========================================

Overview
--------

Catalog-aware Kerberos authentication allows Presto to use different Kerberos authentication rules for different Hive catalogs. This is particularly useful in multi-domain environments where different Hive clusters may belong to different Kerberos realms and require different ``auth_to_local`` rules for principal mapping.

Traditional Kerberos authentication in Presto uses a global configuration that applies to all catalogs, which can cause issues when working with multiple Kerberos domains. The catalog-aware approach solves this by maintaining separate authentication contexts for each catalog.

Configuration
-------------

To enable catalog-aware Kerberos authentication, you must configure the following properties in your Hive catalog configuration files.

Basic Configuration
^^^^^^^^^^^^^^^^^^^

In your catalog properties file (for example, ``etc/catalog/hive.properties``):

.. code-block:: properties

    # Enable Kerberos authentication
    hive.hdfs.authentication.type=KERBEROS
    
    # Enable catalog-aware Kerberos
    hive.catalog-aware-kerberos-enabled=true
    
    # Configure catalog-specific auth_to_local rules
    hive.catalog-auth-to-local-rules=hive_cluster1:RULE:[1:$1@$0](.*@REALM1.COM)s/@.*//,hive_cluster2:RULE:[1:$1@$0](.*@REALM2.COM)s/@.*//

Multi-Catalog Setup
^^^^^^^^^^^^^^^^^^^

For a multi-catalog setup with different Kerberos realms:

**Catalog 1 (etc/catalog/hive-cluster1.properties):**

.. code-block:: properties

    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://metastore1.realm1.com:9083
    hive.hdfs.authentication.type=KERBEROS
    hive.catalog-aware-kerberos-enabled=true
    hive.catalog-auth-to-local-rules=hive_cluster1:RULE:[1:$1@$0](.*@REALM1.COM)s/@.*//
    
    # Kerberos configuration
    hive.hdfs.presto.principal=presto@REALM1.COM
    hive.hdfs.presto.keytab=/etc/presto/presto.keytab

**Catalog 2 (etc/catalog/hive-cluster2.properties):**

.. code-block:: properties

    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://metastore2.realm2.com:9083
    hive.hdfs.authentication.type=KERBEROS
    hive.catalog-aware-kerberos-enabled=true
    hive.catalog-auth-to-local-rules=hive_cluster2:RULE:[1:$1@$0](.*@REALM2.COM)s/@.*//
    
    # Kerberos configuration
    hive.hdfs.presto.principal=presto@REALM2.COM
    hive.hdfs.presto.keytab=/etc/presto/presto-realm2.keytab

Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^

``hive.catalog-aware-kerberos-enabled``
    **Type:** ``boolean``
    
    **Default:** ``false``
    
    **Description:** Enable catalog-aware Kerberos authentication. When enabled, each catalog can have its own Kerberos authentication context with separate ``auth_to_local`` rules.

``hive.catalog-auth-to-local-rules``
    **Type:** ``string``
    
    **Default:** (empty)
    
    **Description:** Comma-separated list of catalog-specific ``auth_to_local`` rules in the format ``catalog_name:auth_to_local_rule``. Each catalog can have its own rule for mapping Kerberos principals to local usernames.

Auth-to-Local Rule Format
^^^^^^^^^^^^^^^^^^^^^^^^^

The ``auth_to_local`` rules follow the standard Hadoop format:

.. code-block:: text

    RULE:[n:string](regexp)s/pattern/replacement/g

Where:

- ``n`` is the number of components in the principal name
- ``string`` is a printf-style string to generate the username
- ``regexp`` is a regular expression to match the principal
- ``pattern/replacement`` is a sed-style substitution

Common Examples:

.. code-block:: properties

    # Map all principals from REALM1.COM, removing the realm
    RULE:[1:$1@$0](.*@REALM1.COM)s/@.*//
    
    # Map service principals to specific users
    RULE:[2:$1@$0](hdfs@REALM1.COM)s/.*/hdfs/
    
    # Default rule for any principal
    DEFAULT

Usage Examples
--------------

Cross-Domain Query
^^^^^^^^^^^^^^^^^^

With catalog-aware Kerberos authentication, you can query across different Kerberos domains:

.. code-block:: sql

    -- Query data from cluster in REALM1.COM
    SELECT count(*) FROM hive_cluster1.default.users;
    
    -- Query data from cluster in REALM2.COM  
    SELECT count(*) FROM hive_cluster2.default.orders;
    
    -- Join data across different Kerberos realms
    SELECT u.name, o.total
    FROM hive_cluster1.default.users u
    JOIN hive_cluster2.default.orders o ON u.id = o.user_id;

Troubleshooting
---------------

Common Issues
^^^^^^^^^^^^^

**Authentication Failures**

If you encounter authentication failures, check:

1. Verify that the keytab files are accessible and valid
2. Ensure the principal names match the configured values
3. Check that the ``auth_to_local`` rules are correctly formatted
4. Verify network connectivity to the KDC servers

**Principal Mapping Issues**

If principal mapping fails:

1. Test the ``auth_to_local`` rules using the ``hadoop`` command:

   .. code-block:: bash

       hadoop org.apache.hadoop.security.HadoopKerberosName principal@REALM.COM

2. Verify that the rules match your principal format
3. Check the order of rules - they are processed sequentially

**Configuration Validation**

To validate your configuration:

1. Enable debug logging for Kerberos authentication:

   .. code-block:: properties

       # In etc/log.properties
       com.facebook.presto.hive.authentication=DEBUG

2. Check the Presto logs for authentication-related messages
3. Verify that each catalog is using the correct authentication context

Best Practices
--------------

1. **Keytab Management**: Use separate keytab files for different realms when possible
2. **Rule Testing**: Test ``auth_to_local`` rules thoroughly before deployment
3. **Monitoring**: Monitor authentication metrics and logs for issues
4. **Security**: Ensure keytab files have appropriate permissions (600)
5. **Documentation**: Document the mapping between catalogs and Kerberos realms

Migration from Traditional Kerberos
-----------------------------------

To migrate from traditional Kerberos authentication:

1. **Backup Configuration**: Save your current configuration files
2. **Enable Feature**: Set ``hive.catalog-aware-kerberos-enabled=true``
3. **Configure Rules**: Add catalog-specific ``auth_to_local`` rules
4. **Test Gradually**: Test with one catalog at a time
5. **Monitor**: Watch for authentication issues during the transition

The catalog-aware feature is backward compatible - catalogs without specific rules will use the default Hadoop configuration.

Security Considerations
-----------------------

- Each catalog maintains its own Kerberos authentication context
- Principal mapping is isolated between catalogs
- Cross-realm trust relationships are handled at the Kerberos level
- Keytab files should be secured with appropriate file permissions
- Regular rotation of keytab files is recommended

Performance Impact
------------------

Catalog-aware Kerberos authentication has minimal performance impact:

- Authentication contexts are cached per catalog
- Principal mapping is performed once per authentication
- No additional network calls are required
- Memory usage increases slightly due to per-catalog caching