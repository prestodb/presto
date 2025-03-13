namespace java com.facebook.presto.experimental
namespace cpp protocol

include "Common.thrift"
include "Sql.thrift"

struct ThriftSessionRepresentation {
  1: string queryId;
  2: optional ThriftTransactionId transactionId;
  3: bool clientTransactionSupport;
  4: string user;
  5: optional string principal;
  6: optional string source;
  7: optional string catalog;
  8: optional string schema;
  9: optional string traceToken;
  10: ThriftTimeZoneKey timeZoneKey;
  11: string locale;
  12: optional string remoteUserAddress;
  13: optional string userAgent;
  14: optional string clientInfo;
  15: set<string> clientTags;
  16: long startTime;
  17: ResourceEstimates resourceEstimates;
  18: map<string, string> systemProperties;
  19: map<ConnectorId, map<string, string>> catalogProperties;
  20: map<string, map<string, string>> unprocessedCatalogProperties;
  21: map<string, ThriftSelectedRole> roles;
  22: map<string, string> preparedStatements;
  23: map<ThriftSqlFunctionId, ThriftSqlInvokedFunction> sessionFunctions;
}