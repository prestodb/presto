namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "Sql.thrift"
include "TypeSignature.thrift"

struct ThriftSessionRepresentation {
  1: string queryId;
  2: optional Common.ThriftTransactionId transactionId;
  3: bool clientTransactionSupport;
  4: string user;
  5: optional string principal;
  6: optional string source;
  7: optional string catalog;
  8: optional string schema;
  9: optional string traceToken;
  10: Common.ThriftTimeZoneKey timeZoneKey;
  11: string locale;
  12: optional string remoteUserAddress;
  13: optional string userAgent;
  14: optional string clientInfo;
  15: set<string> clientTags;
  16: i64 startTime;
  17: Common.ThriftResourceEstimates resourceEstimates;
  18: map<string, string> systemProperties;
  19: map<string, map<string, string>> catalogProperties;
  20: map<string, map<string, string>> unprocessedCatalogProperties;
  21: map<string, Common.ThriftSelectedRole> roles;
  22: map<string, string> preparedStatements;
  23: map<TypeSignature.ThriftSqlFunctionId, Sql.ThriftSqlInvokedFunction> sessionFunctions;
}