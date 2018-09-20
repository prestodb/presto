# Presto OraclePlugin

This is a plugin for Presto that allow you to use Oracle Jdbc Connection

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
Oracle’s JDBC drivers may be obtained from Oracle’s [https://www.oracle.com/technetwork/database/application-development/jdbc/downloads/index.htm](https://www.oracle.com/technetwork/database/application-development/jdbc/downloads/index.htm)

Once you have the Oracle JDBC driver downloaded, you can add that driver to local maven repository using the following command.
    mvn install:install-file -Dfile=<PATH>/ojdbc8.jar -DgroupId=com.oracle -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar
Note that you will have to update the PATH to the appropriate location where you have the JDBC file downloaded. In addition, the version can be specified
as well. Make sure you update the pom file for presto-oracle module appropriately if you do that.
