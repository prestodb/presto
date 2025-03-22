namespace java com.facebook.presto.experimental
namespace cpp protocol

include "TypeSignature.thrift"
include "Signature.thrift"
include "Sql.thrift"

struct ThriftSqlFunctionId {
  1: TypeSignature.ThriftQualifiedObjectName functionName;
  2: list<TypeSignature.ThriftTypeSignature> argumentTypes;
}

struct ThriftParameter {
  1: string name;
  2: TypeSignature.ThriftTypeSignature type;
}

struct ThriftLanguage {
  1: string language
}

enum ThriftDeterminism {
  DETERMINISTIC = 0,
  NOT_DETERMINISTIC = 1
}

enum ThriftNullCallClause {
  RETURN_NULL_ON_NULL_INPUT = 0,
  CALLED_ON_NULL_INPUT = 1
}

struct ThriftRoutineCharacteristics {
  1: ThriftLanguage language;
  2: ThriftDeterminism determinism;
  3: ThriftNullCallClause nullCallClause;
}

struct ThriftFunctionVersion {
  1: optional string version;
}

struct ThriftSqlFunctionHandle {
  1: ThriftSqlFunctionId functionId;
  2: string version;
}

struct ThriftSqlInvokedFunction {
  1: list<ThriftParameter> parameters;
  2: string description;
  3: ThriftRoutineCharacteristics routineCharacteristics;
  4: string body;
  5: bool variableArity;
  6: TypeSignature.ThriftSignature signature;
  7: ThriftSqlFunctionId functionId;
  8: ThriftFunctionVersion functionVersion;
  9: optional ThriftSqlFunctionHandle functionHandle;
}

