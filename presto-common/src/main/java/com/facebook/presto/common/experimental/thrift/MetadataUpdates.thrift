namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "ConnectorMetadataUpdateHandle.thrift"

struct ThriftMetadataUpdates {
  1: optional Common.ThriftConnectorId connectorId;
  2: list<ConnectorMetadataUpdateHandle.ThriftConnectorMetadataUpdateHandle> metadataUpdates;
}