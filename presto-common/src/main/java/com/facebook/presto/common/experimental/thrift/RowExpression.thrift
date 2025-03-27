namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "Block.thrift"
include "Type.thrift"
include "FunctionHandle.thrift"
include "TypeSignature.thrift"

enum ThriftRowExpressionType {
  CALL = 1,
  SPECIAL_FORM = 2,
  LAMBDA_DEFINITION = 3,
  INPUT_REFERENCE = 4,
  VARIABLE_REFERENCE = 5,
  CONSTANT = 6
}

struct ThriftRowExpression {
  1: string type;
  2: binary serializedExpression;
}

struct ThriftCallExpression {
  1: optional Common.ThriftSourceLocation sourceLocation;
  2: string displayName;
  3: FunctionHandle.ThriftFunctionHandle functionHandle;
  4: Type.ThriftType returnType;
  5: list<ThriftRowExpression> arguments;
}

enum ThriftForm
{
    IF = 1,
    NULL_IF = 2,
    SWITCH = 3,
    WHEN = 4,
    IS_NULL = 5,
    COALESCE = 6,
    IN = 7,
    AND = 8,
    OR = 9,
    DEREFERENCE = 10,
    ROW_CONSTRUCTOR = 11,
    BIND = 12
}

struct ThriftSpecialFormExpression {
  1: optional Common.ThriftSourceLocation sourceLocation;
  2: ThriftForm form;
  3: Type.ThriftType returnType;
  4: list<ThriftRowExpression> arguments;
}

struct ThriftLambdaDefinitionExpression {
  1: optional Common.ThriftSourceLocation sourceLocation;
  2: list<Type.ThriftType> argumentTypes;
  3: list<string> arguments;
  4: ThriftRowExpression body;
  5: string canonicalizedBody;
}

struct ThriftInputReferenceExpression {
  1: optional Common.ThriftSourceLocation sourceLocation;
  2: i32 field;
  3: Type.ThriftType type;
}

struct ThriftVariableReferenceExpression {
  1: optional Common.ThriftSourceLocation sourceLocation;
  2: string name;
  3: Type.ThriftType type;
}

struct ThriftConstantExpression {
  1: optional Common.ThriftSourceLocation sourceLocation;
  2: Block.ThriftBlock valueBlock;
  3: Type.ThriftType type;
}

enum ThriftSortOrder {
  ASC_NULLS_FIRST = 1,
  ASC_NULLS_LAST = 2,
  DESC_NULLS_FIRST = 3,
  DESC_NULLS_LAST = 4
}

struct ThriftOrdering
{
  1: ThriftVariableReferenceExpression variable;
  2: ThriftSortOrder sortOrder;
}

struct ThriftOrderingScheme
{
  1: list<ThriftOrdering> orderBy;
}