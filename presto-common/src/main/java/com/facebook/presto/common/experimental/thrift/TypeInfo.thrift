namespace java com.facebook.presto.common.experimental
namespace cpp protocol


struct ThriftTypeInfo {
    1: string type;
    2: binary serializedTypeInfo
}

struct ThriftHiveType {
  1: ThriftTypeInfo typeInfo;
}