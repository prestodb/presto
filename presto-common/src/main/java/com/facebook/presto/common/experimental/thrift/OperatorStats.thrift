namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "RuntimeStats.thrift"
include "OperatorInfo.thrift"

struct ThriftDynamicFilterStats {
  1: set<Common.ThriftPlanNodeId> producerNodeIds;
}

struct ThriftOperatorStats {
  1: i32 stageId;
  2: i32 stageExecutionId;
  3: i32 pipelineId;
  4: i32 operatorId;
  5: Common.ThriftPlanNodeId planNodeId;
  6: string operatorType;
  7: i64 totalDrivers;
  8: i64 isBlockedCalls;
  9: i64 isBlockedWallInNanos;
  10: i64 isBlockedCpuInNanos;
  11: i64 isBlockedAllocationInBytes;
  12: i64 addInputCalls;
  13: i64 addInputWallInNanos;
  14: i64 addInputCpuInNanos;
  15: i64 addInputAllocationInBytes;
  16: i64 rawInputDataSizeInBytes;
  17: i64 rawInputPositions;
  18: i64 inputDataSizeInBytes;
  19: i64 inputPositions;
  20: double sumSquaredInputPositions;
  21: i64 getOutputCalls;
  22: i64 getOutputWallInNanos;
  23: i64 getOutputCpuInNanos;
  24: i64 getOutputAllocationInBytes;
  25: i64 outputDataSizeInBytes;
  26: i64 outputPositions;
  27: i64 physicalWrittenDataSizeInBytes;
  28: i64 additionalCpuInNanos;
  29: i64 blockedWallInNanos;
  30: i64 finishCalls;
  31: i64 finishWallInNanos;
  32: i64 finishCpuInNanos;
  33: i64 finishAllocationInBytes;
  34: i64 userMemoryReservationInBytes;
  35: i64 revocableMemoryReservationInBytes;
  36: i64 systemMemoryReservationInBytes;
  37: i64 peakUserMemoryReservationInBytes;
  38: i64 peakSystemMemoryReservationInBytes;
  39: i64 peakTotalMemoryReservationInBytes;
  40: i64 spilledDataSizeInBytes;
  41: optional Common.ThriftBlockedReason blockedReason;
  42: optional OperatorInfo.ThriftOperatorInfo info;
  43: RuntimeStats.ThriftRuntimeStats runtimeStats;
  44: ThriftDynamicFilterStats dynamicFilterStats;
  45: i64 nullJoinBuildKeyCount;
  46: i64 joinBuildKeyCount;
  47: i64 nullJoinProbeKeyCount;
  48: i64 joinProbeKeyCount;
}