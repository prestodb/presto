namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "Connector.thrift"

struct ThriftTaskStatus {
    1: i64 taskInstanceIdLeastSignificantBits;
    2: i64 taskInstanceIdMostSignificantBits;
    3: i64 version;
    4: Common.ThriftTaskState state;
    5: string self;
    6: set<Connector.ThriftLifespan> completedDriverGroups;
    7: list<Common.ThriftExecutionFailureInfo> failures;
    8: i32 queuedPartitionedDrivers;
    9: i32 runningPartitionedDrivers;
    10: double outputBufferUtilization;
    11: bool outputBufferOverutilized;
    12: i64 physicalWrittenDataSizeInBytes;
    13: i64 memoryReservationInBytes;
    14: i64 systemMemoryReservationInBytes;
    15: i64 peakNodeTotalMemoryReservationInBytes;
    16: i64 fullGcCount;
    17: i64 fullGcTimeInMillis;
    18: i64 totalCpuTimeInNanos;
    19: i64 taskAgeInMillis;
    20: i64 queuedPartitionedSplitsWeight;
    21: i64 runningPartitionedSplitsWeight;
}