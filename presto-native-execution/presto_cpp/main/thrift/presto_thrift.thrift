/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace cpp2 facebook.presto.thrift

enum TaskState {
  PLANNED = 0,
  RUNNING = 1,
  FINISHED = 2,
  CANCELED = 3,
  ABORTED = 4,
  FAILED = 5,
}
enum ErrorType {
  USER_ERROR = 0,
  INTERNAL_ERROR = 1,
  INSUFFICIENT_RESOURCES = 2,
  EXTERNAL = 3,
}
enum ErrorCause {
  UNKNOWN = 0,
  LOW_PARTITION_COUNT = 1,
  EXCEEDS_BROADCAST_MEMORY_LIMIT = 2,
}
enum BufferState {
  OPEN = 0,
  NO_MORE_BUFFERS = 1,
  NO_MORE_PAGES = 2,
  FLUSHING = 3,
  FINISHED = 4,
  FAILED = 5,
}
enum BlockedReason {
  WAITING_FOR_MEMORY = 0,
}
enum RuntimeUnit {
  NONE = 0,
  NANO = 1,
  BYTE = 2,
}
enum JoinType {
  INNER = 0,
  PROBE_OUTER = 1,
  LOOKUP_OUTER = 2,
  FULL_OUTER = 3,
}
enum Type {
  ROLE = 0,
  ALL = 1,
  NONE = 2,
}
enum Determinism {
  DETERMINISTIC = 0,
  NOT_DETERMINISTIC = 1,
}
enum NullCallClause {
  RETURNS_NULL_ON_NULL_INPUT = 0,
  CALLED_ON_NULL_INPUT = 1,
}
enum FunctionKind {
  SCALAR = 0,
  AGGREGATE = 1,
  WINDOW = 2,
}
enum BufferType {
  PARTITIONED = 0,
  BROADCAST = 1,
  ARBITRARY = 2,
  DISCARDING = 3,
  SPOOLING = 4,
}
struct SplitWrapper {
  1: string split;
}
struct TableWriteInfoWrapper {
  1: string tableWriteInfo;
}
struct MetadataUpdatesWrapper {
  1: string metadataUpdates;
}
struct Lifespan {
  1: bool grouped;
  2: i32 groupId;
}
struct ErrorLocation {
  1: i32 lineNumber;
  2: i32 columnNumber;
}
struct HostAddress {
  1: string host;
  2: i32 port;
}
struct StageId {
  1: string queryId;
  2: i32 id;
}
struct OutputBufferId {
  1: i32 id;
}
struct PageBufferInfo {
  1: i32 partition;
  2: i64 bufferedPages;
  3: i64 bufferedBytes;
  4: i64 rowsAdded;
  5: i64 pagesAdded;
}
struct PlanNodeId {
  1: string id;
}
struct DistributionSnapshot {
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
struct RuntimeStats {
  1: map<string, RuntimeMetric> metrics;
}
struct ExchangeClientStatus {
  1: i64 bufferedBytes;
  2: i64 maxBufferedBytes;
  3: i64 averageBytesPerRequest;
  4: i64 successfulRequestsCount;
  5: i32 bufferedPages;
  6: bool noMoreLocations;
  7: list<PageBufferClientStatus> pageBufferClientStatuses;
}
struct PageBufferClientStatus {
  1: string uri;
  2: string state;
  3: i64 lastUpdate;
  4: i64 rowsReceived;
  5: i32 pagesReceived;
  6: i64 rowsRejected;
  7: i32 pagesRejected;
  8: i32 requestsScheduled;
  9: i32 requestsCompleted;
  10: i32 requestsFailed;
  11: string httpRequestState;
}
struct LocalExchangeBufferInfo {
  1: i64 bufferedBytes;
  2: i32 bufferedPages;
}
struct TableFinishInfo {
  1: string serializedConnectorOutputMetadata;
  2: bool jsonLengthLimitExceeded;
  3: double statisticsWallTime;
  4: double statisticsCpuTime;
}
struct SplitOperatorInfo {
  1: map<string, string> splitInfoMap;
}
struct HashCollisionsInfo {
  1: double weightedSumSquaredHashCollisions;
  2: double weightedHashCollisions;
  3: double weightedExpectedHashCollisions;
}
struct PartitionedOutputInfo {
  1: i64 rowsAdded;
  2: i64 pagesAdded;
  3: i64 outputBufferPeakMemoryUsage;
}
struct WindowInfo {
  1: list<DriverWindowInfo> windowInfos;
}
struct DriverWindowInfo {
  1: double sumSquaredDifferencesPositionsOfIndex;
  2: double sumSquaredDifferencesSizeOfIndex;
  3: double sumSquaredDifferencesSizeInPartition;
  4: i64 totalPartitionsCount;
  5: i64 totalRowsCount;
  6: i64 numberOfIndexes;
}
struct TableWriterInfo {
  1: i64 pageSinkPeakMemoryUsage;
  2: double statisticsWallTime;
  3: double statisticsCpuTime;
  4: double validationCpuTime;
}
struct TableWriterMergeInfo {
  1: double statisticsWallTime;
  2: double statisticsCpuTime;
}
struct DynamicFilterStats {
  1: set<PlanNodeId> producerNodeIds;
}
struct DriverStats {
  1: Lifespan lifespan;
  2: i64 createTimeInMillis;
  3: i64 startTimeInMillis;
  4: i64 endTimeInMillis;
  5: double queuedTime;
  6: double elapsedTime;
  7: i64 userMemoryReservationInBytes;
  8: i64 revocableMemoryReservationInBytes;
  9: i64 systemMemoryReservationInBytes;
  10: double totalScheduledTime;
  11: double totalCpuTime;
  12: double totalBlockedTime;
  13: bool fullyBlocked;
  14: set<BlockedReason> blockedReasons;
  15: i64 totalAllocationInBytes;
  16: i64 rawInputDataSizeInBytes;
  17: double rawInputReadTime;
  18: i64 rawInputPositions;
  19: i64 processedInputDataSizeInBytes;
  20: i64 processedInputPositions;
  21: i64 outputDataSizeInBytes;
  22: i64 outputPositions;
  23: i64 physicalWrittenDataSizeInBytes;
  24: list<OperatorStats> operatorStats;
}
struct TransactionId {
  1: string uuid;
}
struct TimeZoneKey {
  1: string id;
  2: i16 key;
}
struct ResourceEstimates {
  1: optional double executionTime;
  2: optional double cpuTime;
  3: optional double peakMemory;
  4: optional double peakTaskMemory;
}
struct ConnectorId {
  1: string catalogName;
}
struct SqlFunctionId {
  1: string signature;
}
struct TypeSignature {
  1: string signature;
  2: bool ignore;
}
struct Language {
  1: string language;
}
struct QualifiedObjectName {
  1: string catalogName;
  2: string schemaName;
  3: string objectName;
}
struct TypeVariableConstraint {
  1: string name;
  2: bool comparableRequired;
  3: bool orderableRequired;
  4: string variadicBound;
  5: bool nonDecimalNumericRequired;
}
struct LongVariableConstraint {
  1: string name;
  2: string expression;
}
struct TaskSource {
  1: PlanNodeId planNodeId;
  2: set<ScheduledSplit> splits;
  3: set<Lifespan> noMoreSplitsForLifespan;
  4: bool noMoreSplits;
}
struct ScheduledSplit {
  1: i64 sequenceId;
  2: PlanNodeId planNodeId;
  3: SplitWrapper split;
}
struct TaskStatus {
  1: i64 taskInstanceIdLeastSignificantBits;
  2: i64 taskInstanceIdMostSignificantBits;
  3: i64 version;
  4: TaskState state;
  5: string selfUri;
  6: set<Lifespan> completedDriverGroups;
  7: list<ExecutionFailureInfo> failures;
  8: i32 queuedPartitionedDrivers;
  9: i32 runningPartitionedDrivers;
  10: double outputBufferUtilization;
  11: bool outputBufferOverutilized;
  12: i64 physicalWrittenDataSizeInBytes;
  13: i64 memoryReservationInBytes;
  14: i64 systemMemoryReservationInBytes;
  15: i64 fullGcCount;
  16: i64 fullGcTimeInMillis;
  17: i64 peakNodeTotalMemoryReservationInBytes;
  18: i64 totalCpuTimeInNanos;
  19: i64 taskAgeInMillis;
  20: i64 queuedPartitionedSplitsWeight;
  21: i64 runningPartitionedSplitsWeight;
}
struct ErrorCode {
  1: i32 code;
  2: string name;
  3: ErrorType type;
  4: bool retriable;
}
struct StageExecutionId {
  1: StageId stageId;
  2: i32 id;
}
struct OutputBufferInfo {
  1: string type;
  2: BufferState state;
  3: list<BufferInfo> buffers;
  4: bool canAddBuffers;
  5: bool canAddPages;
  6: i64 totalBufferedBytes;
  7: i64 totalBufferedPages;
  8: i64 totalRowsSent;
  9: i64 totalPagesSent;
}
struct BufferInfo {
  1: OutputBufferId bufferId;
  2: bool finished;
  3: i32 bufferedPages;
  4: i64 pagesSent;
  5: PageBufferInfo pageBufferInfo;
}
struct TaskStats {
  1: i64 createTimeInMillis;
  2: i64 firstStartTimeInMillis;
  3: i64 lastStartTimeInMillis;
  4: i64 lastEndTimeInMillis;
  5: i64 endTimeInMillis;
  6: i64 elapsedTimeInNanos;
  7: i64 queuedTimeInNanos;
  8: i32 totalDrivers;
  9: i32 queuedDrivers;
  10: i32 runningDrivers;
  11: i32 blockedDrivers;
  12: i32 completedDrivers;
  13: double cumulativeUserMemory;
  14: double cumulativeTotalMemory;
  15: i64 userMemoryReservationInBytes;
  16: i64 revocableMemoryReservationInBytes;
  17: i64 systemMemoryReservationInBytes;
  18: i64 peakUserMemoryInBytes;
  19: i64 peakTotalMemoryInBytes;
  20: i64 peakNodeTotalMemoryInBytes;
  21: i64 totalScheduledTimeInNanos;
  22: i64 totalCpuTimeInNanos;
  23: i64 totalBlockedTimeInNanos;
  24: bool fullyBlocked;
  25: set<BlockedReason> blockedReasons;
  26: i64 totalAllocationInBytes;
  27: i64 rawInputDataSizeInBytes;
  28: i64 rawInputPositions;
  29: i64 processedInputDataSizeInBytes;
  30: i64 processedInputPositions;
  31: i64 outputDataSizeInBytes;
  32: i64 outputPositions;
  33: i64 physicalWrittenDataSizeInBytes;
  34: list<PipelineStats> pipelines;
  35: i32 queuedPartitionedDrivers;
  36: i64 queuedPartitionedSplitsWeight;
  37: i32 runningPartitionedDrivers;
  38: i64 runningPartitionedSplitsWeight;
  39: i32 fullGcCount;
  40: i64 fullGcTimeInMillis;
  41: RuntimeStats runtimeStats;
}
struct PipelineStats {
  1: i32 pipelineId;
  2: i64 firstStartTimeInMillis;
  3: i64 lastStartTimeInMillis;
  4: i64 lastEndTimeInMillis;
  5: bool inputPipeline;
  6: bool outputPipeline;
  7: i32 totalDrivers;
  8: i32 queuedDrivers;
  9: i32 queuedPartitionedDrivers;
  10: i64 queuedPartitionedSplitsWeight;
  11: i32 runningDrivers;
  12: i32 runningPartitionedDrivers;
  13: i64 runningPartitionedSplitsWeight;
  14: i32 blockedDrivers;
  15: i32 completedDrivers;
  16: i64 userMemoryReservationInBytes;
  17: i64 revocableMemoryReservationInBytes;
  18: i64 systemMemoryReservationInBytes;
  19: DistributionSnapshot queuedTime;
  20: DistributionSnapshot elapsedTime;
  21: i64 totalScheduledTimeInNanos;
  22: i64 totalCpuTimeInNanos;
  23: i64 totalBlockedTimeInNanos;
  24: bool fullyBlocked;
  25: set<BlockedReason> blockedReasons;
  26: i64 totalAllocationInBytes;
  27: i64 rawInputDataSizeInBytes;
  28: i64 rawInputPositions;
  29: i64 processedInputDataSizeInBytes;
  30: i64 processedInputPositions;
  31: i64 outputDataSizeInBytes;
  32: i64 outputPositions;
  33: i64 physicalWrittenDataSizeInBytes;
  34: list<OperatorStats> operatorSummaries;
  35: list<DriverStats> drivers;
}
struct RuntimeMetric {
  1: string name;
  2: i64 sum;
  3: i64 count;
  4: i64 max;
  5: i64 min;
  6: RuntimeUnit unit;
}
struct JoinOperatorInfo {
  1: JoinType joinType;
  2: list<i64> logHistogramProbes;
  3: list<i64> logHistogramOutput;
  4: optional i64 lookupSourcePositions;
}
struct SessionRepresentation {
  1: string queryId;
  2: optional TransactionId transactionId;
  3: bool clientTransactionSupport;
  4: string user;
  5: optional string principal;
  6: optional string source;
  7: optional string catalog;
  8: optional string schema;
  9: optional string traceToken;
  10: TimeZoneKey timeZoneKey;
  11: string locale;
  12: optional string remoteUserAddress;
  13: optional string userAgent;
  14: optional string clientInfo;
  15: set<string> clientTags;
  16: ResourceEstimates resourceEstimates;
  17: i64 startTime;
  18: map<string, string> systemProperties;
  19: map<ConnectorId, map<string, string>> catalogProperties;
  20: map<string, map<string, string>> unprocessedCatalogProperties;
  21: map<string, SelectedRole> roles;
  22: map<string, string> preparedStatements;
  23: map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
}
struct SelectedRole {
  1: Type type;
  2: optional string role;
}
struct Parameter {
  1: string name;
  2: TypeSignature type;
}
struct RoutineCharacteristics {
  1: Language language;
  2: Determinism determinism;
  3: NullCallClause nullCallClause;
}
struct Signature {
  1: QualifiedObjectName name;
  2: FunctionKind kind;
  3: TypeSignature returnType;
  4: list<TypeSignature> argumentTypes;
  5: bool variableArity;
  6: list<TypeVariableConstraint> typeVariableConstraints;
  7: list<LongVariableConstraint> longVariableConstraints;
}
struct OutputBuffers {
  1: BufferType type;
  2: i64 version;
  3: bool noMoreBufferIds;
  4: map<OutputBufferId, i32> buffers;
}
struct TaskUpdateRequest {
  1: SessionRepresentation session;
  2: map<string, string> extraCredentials;
  3: optional binary fragment;
  4: list<TaskSource> sources;
  5: OutputBuffers outputIds;
  6: optional TableWriteInfoWrapper tableWriteInfo;
}
struct ExecutionFailureInfo {
  1: string type;
  2: string message;
  3: optional ExecutionFailureInfo cause (cpp.ref_type="shared", drift.recursive_reference=true);
  4: list<ExecutionFailureInfo> suppressed;
  5: list<string> stack;
  6: ErrorLocation errorLocation;
  7: ErrorCode errorCode;
  8: HostAddress remoteHost;
  9: ErrorCause errorCause;
}
struct TaskId {
  1: StageExecutionId stageExecutionId;
  2: i32 id;
  3: i32 attemptNumber;
}
union OperatorInfoUnion {
  1: ExchangeClientStatus exchangeClientStatus;
  2: LocalExchangeBufferInfo localExchangeBufferInfo;
  3: TableFinishInfo tableFinishInfo;
  4: SplitOperatorInfo splitOperatorInfo;
  5: HashCollisionsInfo hashCollisionsInfo;
  6: PartitionedOutputInfo partitionedOutputInfo;
  7: JoinOperatorInfo joinOperatorInfo;
  8: WindowInfo windowInfo;
  9: TableWriterInfo tableWriterInfo;
  10: TableWriterMergeInfo tableWriterMergeInfo;
}
struct SqlInvokedFunction {
  1: list<Parameter> parameters;
  2: string description;
  3: RoutineCharacteristics routineCharacteristics;
  4: string body;
  5: bool variableArity;
  6: Signature signature;
  7: SqlFunctionId functionId;
}
struct TaskInfo {
  1: TaskId taskId;
  2: TaskStatus taskStatus;
  3: i64 lastHeartbeatInMillis;
  4: OutputBufferInfo outputBuffers;
  5: set<PlanNodeId> noMoreSplits;
  6: TaskStats stats;
  7: bool needsPlan;
  8: MetadataUpdatesWrapper metadataUpdates;
  9: string nodeId;
}
struct OperatorStats {
  1: i32 stageId;
  2: i32 stageExecutionId;
  3: i32 pipelineId;
  4: i32 operatorId;
  5: PlanNodeId planNodeId;
  6: string operatorType;
  7: i64 totalDrivers;
  8: i64 addInputCalls;
  9: double addInputWall;
  10: double addInputCpu;
  11: i64 addInputAllocationInBytes;
  12: i64 rawInputDataSizeInBytes;
  13: i64 rawInputPositions;
  14: i64 inputDataSizeInBytes;
  15: i64 inputPositions;
  16: double sumSquaredInputPositions;
  17: i64 getOutputCalls;
  18: double getOutputWall;
  19: double getOutputCpu;
  20: i64 getOutputAllocationInBytes;
  21: i64 outputDataSizeInBytes;
  22: i64 outputPositions;
  23: i64 physicalWrittenDataSizeInBytes;
  24: double additionalCpu;
  25: double blockedWall;
  26: i64 finishCalls;
  27: double finishWall;
  28: double finishCpu;
  29: i64 finishAllocationInBytes;
  30: i64 userMemoryReservationInBytes;
  31: i64 revocableMemoryReservationInBytes;
  32: i64 systemMemoryReservationInBytes;
  33: i64 peakUserMemoryReservationInBytes;
  34: i64 peakSystemMemoryReservationInBytes;
  35: i64 peakTotalMemoryReservationInBytes;
  36: i64 spilledDataSizeInBytes;
  37: RuntimeStats runtimeStats;
  38: optional BlockedReason blockedReason;
  39: OperatorInfoUnion infoUnion;
  40: i64 nullJoinBuildKeyCount;
  41: i64 joinBuildKeyCount;
  42: i64 nullJoinProbeKeyCount;
  43: i64 joinProbeKeyCount;
  44: DynamicFilterStats dynamicFilterStats;
  45: i64 isBlockedCalls;
  46: double isBlockedWall;
  47: double isBlockedCpu;
  48: i64 isBlockedAllocationInBytes;
}

service PrestoThrift {
  void fake();
}
