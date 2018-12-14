# Presto Teradata Plugin

This is a plugin for Presto that allow you to use Teradata Jdbc connection to connect to Teradata database.

[![Presto-Connectors Member](https://img.shields.io/badge/presto--connectors-member-green.svg)](http://presto-connectors.ml)

## Connection Configuration

Create new properties file inside etc/catalog dir:

    connector.name=teradata
    # connection-url is the Teradata JDBC URL. You may use different configurations per environment.
    # For more information, please visit
    # [JDBC driver docs](https://developer.teradata.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html)
    connection-url=jdbc:teradata://<host>/TMODE=ANSI,CHARSET=UTF8
    connection-user=<username>
    connection-password=<password>

Note that if you are using Active Directory based user account and wanted Teradata to use AD to validate the credentials supplied, make sure you use the connection-url as below:

    connection-url=jdbc:teradata://<host>/TMODE=ANSI,CHARSET=UTF8,LOGMECH=LDAP
    
If you like to use FASTEXPORT [(details here)](http://developer.teradata.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BGBFBBEG), include the following changes:

    connection-url=jdbc:teradata://<host>/TMODE=ANSI,CHARSET=UTF8,TYPE=FASTEXPORT
    teradata.use-preparedstatement=true

## Building Presto Teradata JDBC Plugin

    mvn clean install
    
## Adding Teradata Driver
Teradata Driver is not available in common repositories, so you will need to download it from Teradata and install manually in your repository.
Teradata’s JDBC drivers may be obtained from Teradata’s download page: [https://downloads.teradata.com/download/connectivity/jdbc-driver](https://downloads.teradata.com/download/connectivity/jdbc-driver).

Once you have the Teradata JDBC driver files downloaded, you can deploy the those files to presto plugin folder on coordinator and worker nodes.
For example, if the teradata driver files were jdbc-16.20.0.jar and config-16.20.0.jar and presto plugin folder is /usr/lib/presto/lib/plugin, use the following command to copy the files to the plugin folder.

    cp jdbc-16.20.0.jar /usr/lib/presto/lib/plugin/teradata
    cp config-16.20.0.jar /usr/lib/presto/lib/plugin/teradata 

Restart the coordinator and worker processes and Teradata connector will work.