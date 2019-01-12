Thrift Connector
================

Thrift Connector makes it possible to integrate with external storage systems without a custom Presto connector implementation.

In order to use Thrift Connector with external system you need to implement `PrestoThriftService` interface defined in `presto-thrift-api` project.
Next, you configure Thrift Connector to point to a set of machines, called thrift servers, implementing it.
As part of the interface implementation thrift servers will provide metadata, splits and data.
Thrift server instances are assumed to be stateless and independent from each other.

Using Thrift Connector over a custom Presto connector can be especially useful in the following cases.

* Java client for a storage system is not available.
By using Thrift as transport and service definition Thrift Connector can integrate with systems written in non-Java languages.

* Storage system's model doesn't easily map to metadata/table/row concept or there are multiple ways to do it.
For example, there are multiple ways how to map data from a key/value storage to relational representation.
Instead of supporting all of the variations in the connector this task can be moved to the external system itself.

* You cannot or don't want to modify Presto code to add a custom connector to support your storage system.

You can find thrift service interface that needs to be implemented together with related thrift structures in `presto-thrift-api` project.
Documentation of [`PrestoThriftService`](../presto-thrift-api/src/main/java/io/prestosql/connector/thrift/api/PrestoThriftService.java) is a good starting point.
