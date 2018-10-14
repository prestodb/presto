# Presto OraclePlugin

This is a plugin for Presto that allow you to use Oracle Jdbc Connection. Note that this code base doesn't include Oracle JDBC driver. So once you build presto code base, you will have to manually deploy Oracle JDBC jar file to presto plugin folder (details are below) before the connector will work.

[![Presto-Connectors Member](https://img.shields.io/badge/presto--connectors-member-green.svg)](http://presto-connectors.ml)

## Connection Configuration

Create new properties file inside etc/catalog dir:

    connector.name=oracle
    # connection-url must me the URL to access Oracle via JDBC. It can be different depending on your environment.
    # Another example of the URL would be jdbc:oracle:thin:@//ip:port/database. For more information, please go to the JDBC driver docs
    connection-url=jdbc:oracle:thin://ip:port/database
    connection-user=myuser
    connection-password=

## Building Presto Oracle JDBC Plugin

    mvn clean install
    

## Adding Oracle Driver
Oracle Driver is not available in common repositories, so you will need to download it from Oracle and install manually in your repository.
Oracle’s JDBC drivers may be obtained from Oracle’s download page: [https://www.oracle.com/technetwork/database/application-development/jdbc/downloads/index.htm](https://www.oracle.com/technetwork/database/application-development/jdbc/downloads/index.htm)

Once you have the Oracle JDBC driver downloaded, you can deploy the oracle jdbc jar file to presto plugin folder on coordinator and worker nodes.
For example, if the oracle driver file is ojdbc8.jar and presto plugin folder is /usr/lib/presto/lib/plugin, use the following command to copy the library to the plugin folder.

    cp ojdbc8.jar /usr/lib/presto/lib/plugin/oracle 

Restart the coordinator and worker processes and Oracle connector will work.