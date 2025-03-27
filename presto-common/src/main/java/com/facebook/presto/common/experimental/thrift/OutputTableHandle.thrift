namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Connector.thrift"
include "ConnectorTransactionHandle.thrift"
include "ConnectorOutputTableHandle.thrift"

struct ThriftOutputTableHandle {
  1: Connector.ThriftConnectorId connectorId;
  2: ConnectorTransactionHandle.ThriftConnectorTransactionHandle transactionHandle;
  3: ConnectorOutputTableHandle.ThriftConnectorOutputTableHandle connectorHandle;
}