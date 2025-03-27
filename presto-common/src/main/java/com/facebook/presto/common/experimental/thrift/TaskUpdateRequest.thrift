namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "SessionRepresentation.thrift"
include "Connector.thrift"

struct ThriftTaskUpdateRequest {
  1: SessionRepresentation.ThriftSessionRepresentation session;
  2: map<string, string> extraCredentials;
  3: optional binary fragment;
  4: list<Connector.ThriftTaskSource> sources;
  5: Connector.ThriftOutputBuffers outputIds;
  6: optional Connector.ThriftTableWriteInfo tableWriteInfo;
}