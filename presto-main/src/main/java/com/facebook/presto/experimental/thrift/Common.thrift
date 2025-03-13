namespace java com.facebook.presto.experimental
namespace cpp protocol

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
    EXCEEDS_BROADCAST_MEMORY_LIMIT = 2;
}

enum ThriftBufferType {
  BROADCAST = 0,
  PARTITIONED = 1,
  ARBITRARY = 2
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

struct ThriftOutputBufferId {
  1: required i32 id;
}

struct ThriftOutputBuffers {
  1: ThriftBufferType type;
  2: bool noMoreBufferIds;
  3: i64 version;
  4: map<ThriftOutputBufferId, string> buffers;
  5: optional i32 totalBufferCount;
  6: optional i32 totalPartitionCount;
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
  3: optional ThriftExecutionFailureInfo cause;
  4: list<ThriftExecutionFailureInfo> suppressed;
  5: list<string> stack;
  6: ThriftErrorLocation errorLocation;
  7: ThriftErrorCode errorCode;
  8: ThriftHostAddress remoteHost;
  9: ThriftErrorCause errorCause;
}

struct ThriftPlanNodeId {
  1: string id;
}

struct ThriftLifespan {
  1: bool grouped;
  2: optional i32 groupId;
}

struct ThriftScheduledSplit {
  1: i64 sequenceId;
  2: ThriftSplit split;
  3: ThriftLifespan lifespan;
}

struct ThriftSplit {
  1: string connectorId;
  2: binary connectorSplit;
  3: bool remoteSplit;
  4: optional double splitWeight;
}

struct ThriftTaskSource {
  1: ThriftPlanNodeId planNodeId;
  2: list<ThriftScheduledSplit> splits;
  3: bool noMoreSplits;
}

struct ThriftTableWriteInfo {
  1: string catalogName;
  2: string schemaName;
  3: string tableName;
  4: bool writtenByQuery;
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
  3: optional string stringRepresentation;
}

struct ThriftTimeZoneKey {
  1: i16 key;
  2: string id;
}