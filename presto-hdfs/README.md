# Presto HDFS Connector

The HDFS Connector turns HDFS into a reliable and scalable storage for data with schema provided by MetaServer.

## Connector in detail

After analyzing implementations of other connectors such as Hive and JDBC, we give a guidance on how to implement your
own connector.

### Design
When implementing a new Presto plugin, implement interfaces and override methods defined by the SPI.

Plugins can provide additional Connectors, Types, Functions and System Access Control. Currently, we focus on Connector
mostly.

#### Plugin
com.facebook.presto.spi.Plugin

### Implementation

### Roadmap