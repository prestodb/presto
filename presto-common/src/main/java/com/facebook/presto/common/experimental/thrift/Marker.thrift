namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Type.thrift"
include "Block.thrift"

enum ThriftBound
{
  BELOW = 1,
  EXACTLY = 2,
  ABOVE = 3
}

struct ThriftMarker {
  1: Type.ThriftType type;
  2: optional Block.ThriftBlock valueBlock;
  3: ThriftBound bound;
}

struct ThriftRange {
  1: ThriftMarker low;
  2: ThriftMarker high;
}