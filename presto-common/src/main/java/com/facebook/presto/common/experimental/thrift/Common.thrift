namespace java com.facebook.presto.common.experimental
namespace cpp protocol

struct ThriftSourceLocation {
  1: i32 line;
  2: i32 column;
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
  3: ThriftExecutionFailureInfo cause;
  4: list<ThriftExecutionFailureInfo> suppressed;
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