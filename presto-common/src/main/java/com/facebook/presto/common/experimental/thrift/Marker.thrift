namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Type.thrift"
include "Block.thrift"

enum Bound
{
  BELOW = 1,
  EXACTLY = 2,
  ABOVE = 3
}

struct ThriftMarker {
  1: Type.ThriftType type;
  2: optional Block.ThriftBlock valueBlock;
  3: Bound bound;
}

struct ThriftRange {
  1: ThriftMarker low;
  2: ThriftMarker high;
}