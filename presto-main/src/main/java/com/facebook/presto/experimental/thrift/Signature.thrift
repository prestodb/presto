namespace java com.facebook.presto.experimental
namespace cpp protocol

include "TypeSignature.thrift"

enum ThriftFunctionKind {
  SCALA = 0,
  AGGREGATE = 1,
  WINDOW = 2,
  TABLE = 3
}

struct ThriftTypeVariableConstraint {
  1: string name;
  2: bool comparableRequired;
  3: bool orderableRequired;
  4: string variadicBound;
  5: bool nonDecimalNumericRequired;
}

struct ThriftLongVariableConstraint {
  1: string name;
  2: string expression;
}

struct ThriftSignature {
  1: ThriftQualifiedObjectName name;
  2: ThriftFunctionKind kind;
  3: list<ThriftTypeVariableConstraint> typeVariableConstraints;
  4: list<ThriftLongVariableConstraint> longVariableConstraints;
  5: ThriftTypeSignature returnType;
  6: list<ThriftTypeSignature> argumentTypes;
  7: bool variableArity;
}
