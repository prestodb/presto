namespace java com.facebook.presto.experimental
namespace cpp protocol

include "TypeSignatureReference.thrift"

enum ParameterKind {
    TYPE = 1,
    NAMED_TYPE = 2,
    LONG = 3,
    VARIABLE = 4,
    LONG_ENUM = 5,
    VARCHAR_ENUM = 6,
    DISTINCT_TYPE = 7;
}

struct ThriftQualifiedObjectName {
  1: string catalogName;
  2: string schemaName;
  3: string objectName;
}

struct ThriftTypeSignatureBase {
  1: optional string standardTypeBase;
  2: optional ThriftQualifiedObjectName typeName;
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
  1: required string name;
  2: ThriftTypeSignature typeSignature;
}

struct ThriftDistinctTypeInfo {
  1: ThriftQualifiedObjectName name;
  2: ThriftTypeSignature baseType;
  3: bool isOrderable;
  4: optional ThriftQualifiedObjectName topMostAncestor;
  5: list<ThriftQualifiedObjectName> otherAncestors;
}

union ThriftTypeSignatureParameter {
  1: ParameterKind kind;
  2: TypeSignatureReference.ThriftTypeSignatureReference typeSignature;
  3: ThriftNamedTypeSignature namedTypeSignature;
  4: i64 longLiteral;
  5: string variable;
  6: ThriftLongEnumMap longEnumMap;
  7: ThriftVarcharEnumMap varcharEnumMap;
  8: ThriftDistinctTypeInfo distinctTypeInfo;
}

struct ThriftTypeSignature {
  1: ThriftTypeSignatureBase base;
  2: list<ThriftTypeSignatureParameter> parameters;
  3: bool calculated;

  4: string stringRepresentation;
}
