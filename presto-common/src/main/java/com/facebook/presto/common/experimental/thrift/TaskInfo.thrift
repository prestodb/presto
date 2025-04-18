namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "Task.thrift"
include "TaskStatus.thrift"
include "TaskStats.thrift"
include "MetadataUpdates.thrift"

enum ThriftBufferState {
  OPEN = 0,
  NO_MORE_BUFFERS = 1,
  NO_MORE_PAGES = 2,
  FLUSHING = 3,
  FINISHED = 4,
  FAILED = 5,
}

struct ThriftPageBufferInfo
{
  1: i32 partition;
  2: i64 bufferedPages;
  3: i64 bufferedBytes;
  4: i64 rowsAdded;
  5: i64 pagesAdded;
}

struct ThriftBufferInfo {
  1: Common.ThriftOutputBufferId bufferId;
  2: bool finished;
  3: i32 bufferedPages;
  4: i64 pagesSent;
  5: ThriftPageBufferInfo pageBufferInfo;
}

struct ThriftOutputBufferInfo {
  1: string type;
  2: ThriftBufferState state;
  3: bool canAddBuffers;
  4: bool canAddPages;
  5: i64 totalBufferedBytes;
  6: i64 totalBufferedPages;
  7: i64 totalRowsSent;
  8: i64 totalPagesSent;
  9: list<ThriftBufferInfo> buffers;
}

struct ThriftTaskInfo {
  1: Task.ThriftTaskId taskId;
  2: TaskStatus.ThriftTaskStatus taskStatus;
  3: i64 lastHeartbeatInMillis;
  4: ThriftOutputBufferInfo outputBuffers;
  5: set<Common.ThriftPlanNodeId> noMoreSplits;
  6: TaskStats.ThriftTaskStats stats;
  7: bool needsPlan;
  8: MetadataUpdates.ThriftMetadataUpdates metadataUpdates;
  9: string nodeId;
}