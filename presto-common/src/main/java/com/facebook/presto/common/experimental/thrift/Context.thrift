namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "TupleDomain.thrift"

struct ThriftSplitContext {
  1: bool cacheable;
}