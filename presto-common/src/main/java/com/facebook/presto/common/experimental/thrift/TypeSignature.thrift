namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"

enum ThriftParameterKind {
    TYPE = 1,
    NAMED_TYPE = 2,
    LONG = 3,
    VARIABLE = 4,
    LONG_ENUM = 5,
    VARCHAR_ENUM = 6,
    DISTINCT_TYPE = 7
}

struct ThriftRowFieldName {
  1: string name;
  2: bool delimited;
}

struct ThriftTypeSignatureBase {
  1: optional string standardTypeBase;
  2: optional Common.ThriftQualifiedObjectName typeName;
}

struct ThriftVarcharEnumMap {
  1: string typeName;
  2: map<string, string> enumMap;
}

struct ThriftLongEnumMap {
  1: string typeName;
  2: map<string, i64> enumMap;
}

struct ThriftNamedTypeSignature {
  1: optional ThriftRowFieldName rowFieldName;
  2: ThriftTypeSignature typeSignature;
}

struct ThriftDistinctTypeInfo {
  1: Common.ThriftQualifiedObjectName name;
  2: ThriftTypeSignature baseType;
  3: bool isOrderable;
  4: optional Common.ThriftQualifiedObjectName topMostAncestor;
  5: list<Common.ThriftQualifiedObjectName> otherAncestors;
}

union ThriftTypeSignatureParameterUnion {
  1: ThriftParameterKind kind;
  2: ThriftTypeSignature typeSignature;
  3: ThriftNamedTypeSignature namedTypeSignature;
  4: i64 longLiteral;
  5: string variable;
  6: ThriftLongEnumMap longEnumMap;
  7: ThriftVarcharEnumMap varcharEnumMap;
  8: ThriftDistinctTypeInfo distinctTypeInfo;
}

struct ThriftTypeSignatureParameter {
  1: ThriftParameterKind kind;
  2: ThriftTypeSignatureParameterUnion thriftTypeSignatureParameterUnion
}

struct ThriftTypeSignature {
  1: ThriftTypeSignatureBase base;
  2: list<ThriftTypeSignatureParameter> parameters;
  3: bool calculated;
}

struct ThriftSqlFunctionId {
  1: Common.ThriftQualifiedObjectName functionName;
  2: list<ThriftTypeSignature> argumentTypes;
}

struct ThriftSqlFunctionHandle {
  1: ThriftSqlFunctionId functionId;
  2: string version;
}