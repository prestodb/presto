namespace java com.facebook.presto.common.experimental
namespace cpp protocol

struct ThriftObject {
  1: string type
  2: binary serializedObject
}