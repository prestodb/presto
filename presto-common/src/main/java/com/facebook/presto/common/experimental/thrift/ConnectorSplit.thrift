namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "ConnectorTableHandle.thrift"
include "TupleDomain.thrift"
include "Task.thrift"

struct ThriftConnectorSplit {
  1: string type;
  2: binary serializedSplit;
}

struct ThriftTpchSplit {
  1: ConnectorTableHandle.ThriftTpchTableHandle tableHandle;
  2: i32 totalParts;
  3: i32 partNumber;
  4: list<Common.ThriftHostAddress> addresses;
  5: TupleDomain.ThriftTupleDomain predicate;
}

struct ThriftRemoteSplit {
  1: string location;
  2: Task.ThriftTaskId remoteSourceTaskId;
}

struct ThriftTpcdsSplit {
  1: ConnectorTableHandle.ThriftTpcdsTableHandle tableHandle;
  2: i32 totalParts;
  3: i32 partNumber;
  4: list<Common.ThriftHostAddress> addresses;
  5: bool noSexism;
}