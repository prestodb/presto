namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "Signature.thrift"
include "Sql.thrift"
include "TypeSignature.thrift"

enum ThriftFunctionHandleType {
  BUILTIN_HANDLE = 1,
  HIVE_HANDLE = 2,
  NATIVE_HANDLE = 3,
  REST_HANDLE = 4,
  SESSION_HANDLE = 5,
  SQL_HANDLE = 6
}

struct ThriftFunctionHandle {
  1: string type;
  2: binary serializedFunctionHandle;
}

struct ThriftBuiltInFunctionHandle {
  1: Signature.ThriftSignature signature;
}

struct ThriftHiveFunctionHandle {
  1: Signature.ThriftSignature signature;
}

struct ThriftNativeFunctionHandle {
  1: TypeSignature.ThriftSqlFunctionHandle base;
  2: Signature.ThriftSignature signature;
}

struct ThriftRestFunctionHandle {
  1: TypeSignature.ThriftSqlFunctionHandle base;
  2: Signature.ThriftSignature signature;
}

struct ThriftSessionFunctionHandle {
  1: Sql.ThriftSqlInvokedFunction sqlFunction;
}