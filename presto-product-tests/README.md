# Presto product tests

## Test configuration

Test assume that you have hadoop and presto cluster running. For setting up development environment we
recommend presto-docker-devenv project. This presto-docker-devenv contains tools for building and running
docker images to be used on local developer machine for easy setup of presto instance together with dependencies
like hadoop. You can also start presto on your machine locally from IDE (please check main presto README).

The MySQL and PostgreSQL connector tests require that you setup MySQL and PostgreSQL servers.  If you
wish to run these tests, you will need to:
 - install and configuring the database servers
 - add the connection information for each server to test-configuration-local.yaml
 - enable the connectors in Presto by uploading the connector configuration files to each node.

If you do not want to run the MySQL and PostgreSQL tests, add the following to the test execution command line:
   --exclude-groups mysql_connector,postgresql_connector

To make it work you need either:
 - define environment variables for hadoop ${HADOOP_MASTER} and presto ${PRESTO_MASTER} with their IP numbers,
and ${PRESTO_PRODUCT_TESTS_ROOT} with path to the presto product test directory
 - create test-configuration-local.yaml with following (example content):

```
databases:
  hive:
    host: 172.16.2.10

  presto:
    host: 192.168.205.1
```

## Running tests

Product tests are not run by default. To start them use run following command:

```
java -jar target/presto-product-tests-*-executable.jar --config-local file://`pwd`/tempto-configuration-local.yaml
```
or to run tests with mysql, postgres and qurantine groups excluded:
```
java -jar target/presto-product-tests-*-executable.jar --exclude-groups mysql_connector,postgresql_connector,quarantine --config-local file://`pwd`/tempto-configuration-local.yaml
```

