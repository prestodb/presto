==========
LDAP setup
==========

LDAP authentication can be enabled on presto server. Add the following configurations to etc/config.properties.

.. code-block:: none

	http-server.https.enabled=true
	http-server.https.port=8443
	http-server.https.keystore.path=/path/to/keystore
	http-server.https.keystore.key=<keystore password>
	ldap.url=ldaps://myhost:636/
	ldap.systemUsername=<active directory service account>
	ldap.systemPassword=<active directory service password> 
	ldap.searchBase="DC=corp,DC=root,DC=mycompany,DC=com"
	ldap.authentication.enabled=true

To authenticate from presto jdbc client, change JDBC URL to below using the above configuration.

.. code-block:: none

	jdbc:presto://host:8443 and enter user id/password in the client tool.

Add the following properties to system properties to SQL client tool to point to keystore.
 
.. code-block:: none

	java -jar sqlworkbench.jar -Djavax.net.ssl.keyStorePassword=<keystore password> -Djavax.net.ssl.keyStore=/path/to/keystore

To authenticate from presto client tool, add these command line options.

.. code-block:: none

	--server https://host:8443 
	--ldap-user <user id>  
	--ldap-password <password> 
	--keystore-path <path/to/keystore> 
	--keystore-password <keystore password>

Finally you need to add custom LDAP validation code as the validation is specific to each enterprise. See ActiveDirectoryAuthenticationModule.java in presto-main subproject.
You want to add the validation code in ActiveDirectoryRealm::queryForAuthentication() starting line 109.
