namespace java com.facebook.presto.experimental
namespace cpp protocol

include "Common.thrift"
include "SessionRepresentation.thrift"

struct ThriftTaskUpdateRequest {
  1: SessionRepresentation.ThriftSessionRepresentation session;
  2: map<string, string> extraCredentials;
  3: optional binary fragment;
  4: list<Common.ThriftTaskSource> sources;
  5: Common.ThriftOutputBuffers outputIds;
  6: optional Common.ThriftTableWriteInfo tableWriteInfo;
}