namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Type.thrift"
include "ColumnHandle.thrift"
include "TupleDomain.thrift"
include "RowExpression.thrift"
include "Common.thrift"

struct ThriftConnectorTableLayoutHandle {
  1: string type;
  2: binary serializedConnectorTableLayoutHandle;
}

struct ThriftPartitionNameWithVersion {
  1: string partitionName;
  2: optional i64 partitionVersion;
}

struct ThriftHivePartition {
  1: Common.ThriftSchemaTableName tableName;
  2: ThriftPartitionNameWithVersion partitionId;
  3: map<ColumnHandle.ThriftColumnHandle, Type.ThriftNullableValue> keys;
}

struct ThriftBaseHiveTableLayoutHandle {
  1: list<ColumnHandle.ThriftBaseHiveColumnHandle> partitionColumns;
  2: TupleDomain.ThriftTupleDomain domainPredicate;
  3: RowExpression.ThriftRowExpression remainingPredicate;
  4: bool pushdownFilterEnabled;
  5: TupleDomain.ThriftTupleDomain partitionColumnPredicate;
  6: optional list<ThriftHivePartition> partitions;
}