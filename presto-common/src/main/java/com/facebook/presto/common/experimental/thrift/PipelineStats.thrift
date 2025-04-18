namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "OperatorStats.thrift"
include "DriverStats.thrift"

struct ThriftPipelineStats {
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
  19: Common.ThriftDistributionSnapshot queuedTime;
  20: Common.ThriftDistributionSnapshot elapsedTime;
  21: i64 totalScheduledTimeInNanos;
  22: i64 totalCpuTimeInNanos;
  23: i64 totalBlockedTimeInNanos;
  24: bool fullyBlocked;
  25: set<Common.ThriftBlockedReason> blockedReasons;
  26: i64 totalAllocationInBytes;
  27: i64 rawInputDataSizeInBytes;
  28: i64 rawInputPositions;
  29: i64 processedInputDataSizeInBytes;
  30: i64 processedInputPositions;
  31: i64 outputDataSizeInBytes;
  32: i64 outputPositions;
  33: i64 physicalWrittenDataSizeInBytes;
  34: list<OperatorStats.ThriftOperatorStats> operatorSummaries;
  35: list<DriverStats.ThriftDriverStats> drivers;
}