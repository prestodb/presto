namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "Context.thrift"
include "ConnectorTableHandle.thrift"
include "ConnectorTransactionHandle.thrift"
include "ConnectorTableLayoutHandle.thrift"
include "ConnectorSplit.thrift"

typedef string ThriftPlanNodeId
typedef string ThriftConnectorId

typedef i32 ThriftOutputBufferId

enum ThriftBufferType {
  BROADCAST = 0,
  PARTITIONED = 1,
  ARBITRARY = 2
}

struct ThriftOutputBuffers {
  1: ThriftBufferType type;
  2: i64 version;
  3: bool noMoreBufferIds;
  4: map<ThriftOutputBufferId, i32> buffers;
}

struct ThriftLifespan {
  1: bool grouped;
  2: i32 groupId;
}

struct ThriftScheduledSplit {
  1: i64 sequenceId;
  2: ThriftPlanNodeId planNodeId;
  3: ThriftSplit split;
}

struct ThriftSplit {
  1: ThriftConnectorId connectorId;
  2: ConnectorTransactionHandle.ThriftConnectorTransactionHandle transactionHandle;
  3: ConnectorSplit.ThriftConnectorSplit connectorSplit;
  4: ThriftLifespan lifespan;
  5: Context.ThriftSplitContext splitContext;
}

struct ThriftTaskSource {
  1: ThriftPlanNodeId planNodeId;
  2: set<ThriftScheduledSplit> splits;
  3: set<ThriftLifespan> noMoreSplitsForLifespan;
  4: bool noMoreSplits;
}

struct ThriftTableHandle {
  1: ThriftConnectorId connectorId;
  2: ConnectorTableHandle.ThriftConnectorTableHandle connectorTableHandle;
  3: ConnectorTransactionHandle.ThriftConnectorTransactionHandle transactionHandle;
  4: optional ConnectorTableLayoutHandle.ThriftConnectorTableLayoutHandle layout;
}

struct ThriftAnalyzeTableHandle {
  1: ThriftConnectorId connectorId;
  2: ConnectorTransactionHandle.ThriftConnectorTransactionHandle transactionHandle;
  3: ConnectorTableHandle.ThriftConnectorTableHandle connectorHandle;
}

struct ThriftDeleteScanInfo {
  1: ThriftPlanNodeId id;
  2: ThriftTableHandle tableHandle;
}