namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "ColumnHandle.thrift"
include "Type.thrift"
include "TypeInfo.thrift"

struct ThriftConnectorOutputTableHandle {
  1: string type;
  2: binary serializedOutputTableHandle;
}

struct ThriftStorageFormat {
  1: string serDe;
  2: string inputFormat;
  3: string outputFormat;
}

struct ThriftHiveBucketProperty {
  1: list<string> bucketedBy;
  2: i32 bucketCount;
  3: list<ThriftSortingColumn> sortedBy;
  4: ThriftBucketFunctionType bucketFunctionType;
  5: optional list<Type.ThriftType> types;
}

struct ThriftStorage {
  1: ThriftStorageFormat storageFormat;
  2: string location;
  3: optional ThriftHiveBucketProperty bucketProperty;
  4: bool skewed;
  5: map<string, string> serdeParameters;
  6: map<string, string> parameters;
}

struct ThriftColumn {
  1: string name;
  2: TypeInfo.ThriftHiveType type;
  3: optional string comment;
  4: optional string typeMetadata;
}

struct ThriftPartition {
  1: string databaseName;
  2: string tableName;
  3: list<string> values;
  4: ThriftStorage storage;
  5: list<ThriftColumn> columns;
  6: map<string, string> parameters;
  7: optional i64 partitionVersion;
  8: bool eligibleToIgnore;
  9: bool sealedPartition;
  10: i32 createTime;
  11: i64 lastDataCommitTime;
  12: optional binary rowIdPartitionComponent;
}

struct ThriftOptionalPartition {
  1: bool isPresent;
  2: ThriftPartition partition;
}

struct ThriftListOfStringAsKey {
  1: list<string> key;
}

struct ThriftHivePageSinkMetadata {
  1: Common.ThriftSchemaTableName schemaTableName;
  2: optional ThriftTable table;
  3: map<ThriftListOfStringAsKey, ThriftOptionalPartition> modifiedPartitions;
}

enum ThriftTableType
{
    NEW = 0,
    EXISTING = 1,
    TEMPORARY = 2,
}

enum ThriftPrestoTableType {
     MANAGED_TABLE = 0,
     EXTERNAL_TABLE = 1,
     VIRTUAL_VIEW = 2,
     MATERIALIZED_VIEW = 3,
     TEMPORARY_TABLE = 4,
     OTHER = 5,
}

struct ThriftTable {
  1: string databaseName;
  2: string tableName;
  3: string owner;
  4: ThriftPrestoTableType tableType;
  5: list<ThriftColumn> dataColumns;
  6: list<ThriftColumn> partitionColumns;
  7: ThriftStorage storage;
  8: map<string, string> parameters;
  9: optional string viewOriginalText;
  10: optional string viewExpandedText;
}

enum ThriftWriteMode {
    STAGE_AND_MOVE_TO_TARGET_DIRECTORY = 0,
    DIRECT_TO_TARGET_NEW_DIRECTORY = 1,
    DIRECT_TO_TARGET_EXISTING_DIRECTORY = 2,
}

struct ThriftLocationHandle {
  1: string targetPath;
  2: string writePath;
  3: optional string tempPath;
  4: ThriftTableType tableType;
  5: ThriftWriteMode writeMode;
}

enum ThriftOrder
{
    ASCENDING = 0,
    DESCENDING = 1,
}

struct ThriftSortingColumn {
  1: string columnName;
  2: ThriftOrder order;
}

enum ThriftBucketFunctionType
{
    HIVE_COMPATIBLE = 0,
    PRESTO_NATIVE = 1,
}

enum ThriftHiveStorageFormat
{
    ORC = 0,
    DWRF = 1,
    ALPHA = 2,
    PARQUET = 3,
    AVRO = 4,
    RCBINARY = 5,
    RCTEXT = 6,
    SEQUENCEFILE = 7,
    JSON = 8,
    TEXTFILE = 9,
    CSV = 10,
    PAGEFILE = 11,
}

enum ThriftHiveCompressionCodec
{
    NONE = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZ4 = 3,
    ZSTD = 4,
}

struct ThriftDwrfEncryptionMetadata {
  1: map<string, binary (java.type = "byte[]")> fieldToKeyData;
  2: map<string, string> extraMetadata;
  3: string encryptionAlgorithm;
  4: string encryptionProvider;
}

struct ThriftEncryptionInformation {
  1: optional ThriftDwrfEncryptionMetadata dwrfEncryptionMetadata;
}

struct ThriftHiveWritableTableHandle {
  1: string schemaName;
  2: string tableName;
  3: list<ColumnHandle.ThriftHiveColumnHandle> inputColumns;
  4: ThriftHivePageSinkMetadata pageSinkMetadata;
  5: ThriftLocationHandle locationHandle;
  6: optional ThriftHiveBucketProperty bucketProperty;
  7: list<ThriftSortingColumn> preferredOrderingColumns;
  8: ThriftHiveStorageFormat tableStorageFormat;
  9: ThriftHiveStorageFormat partitionStorageFormat;
  10: ThriftHiveStorageFormat actualStorageFormat;
  11: ThriftHiveCompressionCodec compressionCodec;
  12: optional ThriftEncryptionInformation encryptionInformation;
}

struct ThriftHiveOutputTableHandle {
  1: ThriftHiveWritableTableHandle hiveWritableTableHandle
  2: list<string> partitionedBy;
  3: string tableOwner;
  4: map<string, string> additionalTableParameters;
}