namespace java com.facebook.presto.common.experimental
namespace cpp protocol

enum ThriftConnectorTableHandleType {
    BASE_HIVE_HANDLE = 1,
    HIVE_HANDLE = 2
}

struct ThriftConnectorTableHandle {
  1: string type;
  2: binary serializedConnectorTableHandle;
}

struct ThriftBaseHiveTableHandle {
  1: string schemaName;
  2: string tableName;
}

struct ThriftHiveTableHandle {
  1: string schemaName;
  2: string tableName;
  3: optional list<list<string>> analyzePartitionValues;
}

struct ThriftTpchTableHandle {
  1: string tableName;
  2: double scaleFactor;
}