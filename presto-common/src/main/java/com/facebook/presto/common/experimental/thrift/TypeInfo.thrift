namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol


struct ThriftTypeInfo {
    1: string type;
    2: binary serializedTypeInfo
}

struct ThriftHiveType {
  1: ThriftTypeInfo typeInfo;
}

struct ThriftPrimitiveTypeInfo {
  1: string typeName;
}

struct ThriftVarcharTypeInfo {
  1: i32 length;
}

struct ThriftCharTypeInfo {
  1: i32 length;
}

struct ThriftDecimalTypeInfo {
  1: i32 precision;
  2: i32 scale;
}