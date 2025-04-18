namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "OperatorStats.thrift"

struct ThriftDriverStats {
  1: Common.ThriftLifespan lifespan;
  2: i64 createTimeInMillis;
  3: i64 startTimeInMillis;
  4: i64 endTimeInMillis;
  5: i64 queuedTimeInNanos;
  6: i64 elapsedTimeInNanos;
  7: i64 userMemoryReservationInBytes;
  8: i64 revocableMemoryReservationInBytes;
  9: i64 systemMemoryReservationInBytes;
  10: i64 totalScheduledTimeInNanos;
  11: i64 totalCpuTimeInNanos;
  12: i64 totalBlockedTimeInNanos;
  13: bool fullyBlocked;
  14: set<Common.ThriftBlockedReason> blockedReasons;
  15: i64 totalAllocationInBytes;
  16: i64 rawInputDataSizeInBytes;
  17: i64 rawInputReadTimeInNanos;
  18: i64 rawInputPositions;
  19: i64 processedInputDataSizeInBytes;
  20: i64 processedInputPositions;
  21: i64 outputDataSizeInBytes;
  22: i64 outputPositions;
  23: i64 physicalWrittenDataSizeInBytes;
  24: list<OperatorStats.ThriftOperatorStats> operatorStats;
}