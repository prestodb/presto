namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Object.thrift"

struct ThriftType {
  1: string type
  2: binary serializedData
}

struct ThriftNullableValue {
  1: ThriftType type;
  2: optional Object.ThriftObject object;
}