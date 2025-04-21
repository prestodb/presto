namespace java.swift com.facebook.presto.common.experimental.auto_gen
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

struct ThriftBigintType {}

struct ThriftIntegerType {}

struct ThriftVarcharType {
  1: i32 length;
}