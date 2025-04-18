namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "PipelineStats.thrift"
include "RuntimeStats.thrift"

struct ThriftTaskStats {
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
  25: set<Common.ThriftBlockedReason> blockedReasons;
  26: i64 totalAllocationInBytes;
  27: i64 rawInputDataSizeInBytes;
  28: i64 rawInputPositions;
  29: i64 processedInputDataSizeInBytes;
  30: i64 processedInputPositions;
  31: i64 outputDataSizeInBytes;
  32: i64 outputPositions;
  33: i64 physicalWrittenDataSizeInBytes;
  34: list<PipelineStats.ThriftPipelineStats> pipelines;
  35: i32 queuedPartitionedDrivers;
  36: i64 queuedPartitionedSplitsWeight;
  37: i32 runningPartitionedDrivers;
  38: i64 runningPartitionedSplitsWeight;
  39: i32 fullGcCount;
  40: i64 fullGcTimeInMillis;
  41: RuntimeStats.ThriftRuntimeStats runtimeStats;
}