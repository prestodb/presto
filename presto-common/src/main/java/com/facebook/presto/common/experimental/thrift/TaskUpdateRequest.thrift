namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "SessionRepresentation.thrift"
include "Connector.thrift"
include "ExecutionWriterTarget.thrift"

struct ThriftTaskUpdateRequest {
  1: SessionRepresentation.ThriftSessionRepresentation session;
  2: map<string, string> extraCredentials;
  3: optional binary fragment;
  4: list<Connector.ThriftTaskSource> sources;
  5: Connector.ThriftOutputBuffers outputIds;
  6: optional ExecutionWriterTarget.ThriftTableWriteInfo tableWriteInfo;
}