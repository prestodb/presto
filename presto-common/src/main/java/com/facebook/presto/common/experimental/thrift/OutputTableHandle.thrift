namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "Connector.thrift"
include "ConnectorTransactionHandle.thrift"
include "ConnectorOutputTableHandle.thrift"

struct ThriftOutputTableHandle {
  1: Common.ThriftConnectorId connectorId;
  2: ConnectorTransactionHandle.ThriftConnectorTransactionHandle transactionHandle;
  3: ConnectorOutputTableHandle.ThriftConnectorOutputTableHandle connectorHandle;
}