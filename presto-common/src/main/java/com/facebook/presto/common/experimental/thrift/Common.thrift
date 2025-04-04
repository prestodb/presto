namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol


typedef string ThriftPlanNodeId
typedef string ThriftConnectorId
typedef i32 ThriftOutputBufferId

struct BinaryWrapper {
  1: binary data;
}

enum ThriftBlockedReason {
  WAITING_FOR_MEMORY = 0,
}

struct ThriftLifespan {
  1: bool grouped;
  2: i32 groupId;
}

struct ThriftSourceLocation {
  1: i32 line;
  2: i32 column;
}

struct ThriftDistributionSnapshot {
  1: double maxError;
  2: double count;
  3: double total;
  4: i64 p01;
  5: i64 p05;
  6: i64 p10;
  7: i64 p25;
  8: i64 p50;
  9: i64 p75;
  10: i64 p90;
  11: i64 p95;
  12: i64 p99;
  13: i64 min;
  14: i64 max;
  15: double avg;
}

enum ThriftTaskState {
    PLANNED = 0,
    RUNNING = 1,
    FINISHED = 2,
    CANCELED = 3,
    ABORTED = 4,
    FAILED = 5,
}

enum ThriftErrorCause {
    UNKNOWN = 0,
    LOW_PARTITION_COUNT = 1,
    EXCEEDS_BROADCAST_MEMORY_LIMIT = 2
}

enum ThriftErrorType {
  USER_ERROR = 0,
  INTERNAL_ERROR = 1,
  INSUFFICIENT_RESOURCES = 2,
  EXTERNAL = 3,
}

enum ThriftSelectedRoleType {
  ROLE = 1,
  ALL = 2,
  NONE = 3,
}

struct ThriftSelectedRole {
  1: ThriftSelectedRoleType type;
  2: optional string role;
}

struct ThriftErrorCode {
  1: i32 code;
  2: string name;
  3: ThriftErrorType type;
  4: bool retriable;
}

struct ThriftErrorLocation {
  1: i32 lineNumber;
  2: i32 columnNumber;
}

struct ThriftHostAddress {
  1: string host;
  2: i32 port;
}

struct ThriftExecutionFailureInfo {
  1: string type;
  2: string message;
  3: optional ThriftExecutionFailureInfo cause (cpp.ref_type = "shared");
  4: optional list<ThriftExecutionFailureInfo> suppressed;
  5: list<string> stack;
  6: ThriftErrorLocation errorLocation;
  7: ThriftErrorCode errorCode;
  8: ThriftHostAddress remoteHost;
  9: ThriftErrorCause errorCause;
}

struct ThriftResourceEstimates {
  1: optional i64 executionTimeInMillis;
  2: optional i64 cpuTimeInNanos;
  3: optional i64 peakMemoryInBytes;
  4: optional i64 peakTaskMemoryInBytes;
}

struct ThriftTransactionId {
  1: i64 mostSignificantBits;
  2: i64 leastSignificantBits;
}

struct ThriftTimeZoneKey {
  1: string id;
  2: i16 key;
}

struct ThriftQualifiedObjectName {
  1: string catalogName;
  2: string schemaName;
  3: string objectName;
}

struct ThriftSchemaTableName {
  1: string schemaName;
  2: string tableName;
}

struct ThriftSlice {
  1: binary data;
}