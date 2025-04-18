namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "Context.thrift"
include "ConnectorTableHandle.thrift"
include "ConnectorTransactionHandle.thrift"
include "ConnectorTableLayoutHandle.thrift"
include "ConnectorSplit.thrift"

enum ThriftBufferType {
  BROADCAST = 0,
  PARTITIONED = 1,
  ARBITRARY = 2
}

struct ThriftOutputBuffers {
  1: ThriftBufferType type;
  2: i64 version;
  3: bool noMoreBufferIds;
  4: map<Common.ThriftOutputBufferId, i32> buffers;
}

struct ThriftScheduledSplit {
  1: i64 sequenceId;
  2: Common.ThriftPlanNodeId planNodeId;
  3: ThriftSplit split;
}

struct ThriftSplit {
  1: Common.ThriftConnectorId connectorId;
  2: ConnectorTransactionHandle.ThriftConnectorTransactionHandle transactionHandle;
  3: ConnectorSplit.ThriftConnectorSplit connectorSplit;
  4: Common.ThriftLifespan lifespan;
  5: Context.ThriftSplitContext splitContext;
}

struct ThriftTaskSource {
  1: Common.ThriftPlanNodeId planNodeId;
  2: set<ThriftScheduledSplit> splits;
  3: set<Common.ThriftLifespan> noMoreSplitsForLifespan;
  4: bool noMoreSplits;
}

struct ThriftTableHandle {
  1: Common.ThriftConnectorId connectorId;
  2: ConnectorTableHandle.ThriftConnectorTableHandle connectorTableHandle;
  3: ConnectorTransactionHandle.ThriftConnectorTransactionHandle transactionHandle;
  4: optional ConnectorTableLayoutHandle.ThriftConnectorTableLayoutHandle layout;
}

struct ThriftAnalyzeTableHandle {
  1: Common.ThriftConnectorId connectorId;
  2: ConnectorTransactionHandle.ThriftConnectorTransactionHandle transactionHandle;
  3: ConnectorTableHandle.ThriftConnectorTableHandle connectorHandle;
}

struct ThriftDeleteScanInfo {
  1: Common.ThriftPlanNodeId id;
  2: ThriftTableHandle tableHandle;
}