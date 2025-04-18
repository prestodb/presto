namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"

struct ThriftBlock {
  1: string type;
  2: binary serializedBlock;
}

struct ThriftLongArrayBlock {
  1: i32 positionCount;
  2: optional list<bool> valueIsNull;
  3: list<i64> values;
}

struct ThriftVariableWidthBlock {
  1: i32 positionCount;
  2: Common.ThriftSlice slice;
  3: list<i32> offsets;
  4: optional list<bool> valueIsNull;
}