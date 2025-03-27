namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "Type.thrift"
include "RowExpression.thrift"
include "TypeSignature.thrift"
include "TypeInfo.thrift"

enum ThriftPathElementType {
    NESTED_FIELD = 1,
    LONG_SUBSCRIPT = 2,
    STRING_SUBSCRIPT = 3,
    ALL_SUBSCRIPTS = 4,
    NO_SUBFIELD = 5
}

struct ThriftPathElement {
  1: string type;
  2: binary serializedElement;
}

struct ThriftNestedFieldElement {
  1: string name;
}

struct ThriftLongSubscriptElement {
  1: i64 index;
}

struct ThriftStringSubScriptElement {
  1: string index;
}

struct ThriftAllSubscriptsElement {
}

struct ThriftNoSubfieldElement {
}

struct ThriftSubfield {
  1: string name;
  2: list<ThriftPathElement> path;
}

enum ThriftColumnType {
    PARTITION_KEY = 1,
    REGULAR = 2,
    SYNTHESIZED = 3,
    AGGREGATED = 4,
}

enum ThriftColumnHandleType {
    BASE_HIVE_COLUMN_HANDLE = 1,
    HIVE_COLUMN_HANDLE = 2
}

struct ThriftColumnHandle {
  1: string type;
  2: binary serializedHandle;
}

struct ThriftBaseHiveColumnHandle {
  1: string name;
  2: optional string comment;
  3: ThriftColumnType columnType
  4: list<ThriftSubfield> requiredSubfields;
}

struct ThriftHiveColumnHandle {
  1: ThriftBaseHiveColumnHandle baseHandle;
  2: TypeInfo.ThriftHiveType hiveType;
  3: TypeSignature.ThriftTypeSignature typeName;
  4: i32 hiveColumnIndex;
  5: optional ThriftAggregation partialAggregation;
}

struct ThriftAggregation {
  1: RowExpression.ThriftCallExpression call;
  2: optional RowExpression.ThriftRowExpression filter;
  3: optional RowExpression.ThriftOrderingScheme orderingScheme;
  4: bool isDistinct;
  5: optional RowExpression.ThriftVariableReferenceExpression mask;
}