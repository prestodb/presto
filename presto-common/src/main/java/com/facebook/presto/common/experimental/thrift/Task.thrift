namespace java com.facebook.presto.common.experimental
namespace cpp protocol


typedef string ThriftQueryId

struct ThriftStageId
{
  1: ThriftQueryId queryId;
  2: i32 id;
}

struct ThriftStageExecutionId {
  1: ThriftStageId stageId;
  2: i32 id;
}

struct ThriftTaskId {
  1: ThriftStageExecutionId stageExecutionId;
  2: i32 id;
  3: i32 attemptNumber;
}