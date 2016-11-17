# Presto HDFS Connector

The HDFS Connector turns HDFS into a reliable and scalable storage for data with schema provided by MetaServer.

## Connector in detail

After analyzing implementations of other connectors such as Hive and JDBC, we give a guidance on how to implement your
own connector.

### Tips Ahead
Set up Maven and don't forget to read `checkstyle` carefully.

### Design
When implementing a new Presto plugin, implement interfaces and override methods defined by the SPI.

Plugins can provide additional Connectors, Types, Functions and System Access Control. Currently, we focus on Connector
mostly.

#### Plugin
Override `getConnectorFactories` method in `com.facebook.presto.spi.Plugin`, and return a customized `ConnectorFactory`

#### ConnectorFactory
Instances of `Connector` are created by a `ConnectorFactory` in dependency injection way. Additionally, `ConnectorFactory` provides 
a `ConnectorHandleResolver`.

#### Connector
`Connector` returns instances of the following services:
+ `ConnectorMetadata`: metadata interface allowing Presto to get list of schemas, tables, columns, etc.
+ `ConnectorSplitManager`: split manager that partitions table into individual chunks that Presto will distribute to workers for processing.
+ `ConnectorPageSourceProvider`: given transaction, split and columns, the page source provider creates PageSource for memory layout.

Connector methods in detail:
+ `beginTransaction`
+ `commit`
+ `rollback`
+ `getMetadata`
+ `getSplitManager`
+ `getPageSourceProvider`
+ `getPageSinkProvider`
+ `getNodePartitioningProvider`
+ `getSystemTables`
+ `shutdown`

#### ConnectorMetadata

#### ConnectorSplitManager

#### ConnectorPageSourceProvider

### Implementation

### Roadmap