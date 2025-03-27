namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "TypeSignature.thrift"
include "Signature.thrift"


struct ThriftParameter {
  1: string name;
  2: TypeSignature.ThriftTypeSignature type;
}

typedef string ThriftLanguage

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

struct ThriftAggregationFunctionMetadata {
  1: TypeSignature.ThriftTypeSignature intermediateType;
  2: bool isOrderSensitive;
}

struct ThriftSqlInvokedFunction {
  1: list<ThriftParameter> parameters;
  2: string description;
  3: ThriftRoutineCharacteristics routineCharacteristics;
  4: string body;
  5: bool variableArity;
  6: Signature.ThriftSignature signature;
  7: TypeSignature.ThriftSqlFunctionId functionId;
  8: ThriftFunctionVersion functionVersion;
  9: optional TypeSignature.ThriftSqlFunctionHandle functionHandle;
  10: optional ThriftAggregationFunctionMetadata aggregationMetadata;
}

