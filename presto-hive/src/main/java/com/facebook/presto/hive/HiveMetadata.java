/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.LocationService.WriteInfo;
import com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil;
import com.facebook.presto.hive.statistics.HiveStatisticsProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorTablePartitioning;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewNotFoundException;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableLayoutFilterCoverage;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.PrivilegeInfo;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TypeUtils.isNumericType;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.binaryExpression;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.BucketFunctionType.PRESTO_NATIVE;
import static com.facebook.presto.hive.ColumnEncryptionInformation.ColumnWithStructSubfield;
import static com.facebook.presto.hive.DwrfTableEncryptionProperties.forPerColumn;
import static com.facebook.presto.hive.DwrfTableEncryptionProperties.forTable;
import static com.facebook.presto.hive.DwrfTableEncryptionProperties.fromHiveTableProperties;
import static com.facebook.presto.hive.HiveAnalyzeProperties.getPartitionList;
import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.HiveBasicStatistics.createZeroStatistics;
import static com.facebook.presto.hive.HiveBucketHandle.createVirtualBucketHandle;
import static com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucketHandle;
import static com.facebook.presto.hive.HiveClientConfig.InsertExistingPartitionsBehavior;
import static com.facebook.presto.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.ROW_ID_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.rowIdColumnHandle;
import static com.facebook.presto.hive.HiveColumnHandle.updateRowIdHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_COLUMN_ORDER_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_READ_ONLY;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TIMEZONE_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_ENCRYPTION_OPERATION;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveManifestUtils.getManifestSizeInBytes;
import static com.facebook.presto.hive.HiveManifestUtils.updatePartitionMetadataWithFileNamesAndSizes;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.differenceDataPredicates;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.getMaterializedDataPredicates;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.getViewToBasePartitionMap;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.validateMaterializedViewPartitionColumns;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.viewToBaseTableOnOuterJoinSideIndirectMappedPartitions;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HivePartitioningHandle.createHiveCompatiblePartitioningHandle;
import static com.facebook.presto.hive.HivePartitioningHandle.createPrestoNativePartitioningHandle;
import static com.facebook.presto.hive.HiveSessionProperties.HIVE_STORAGE_FORMAT;
import static com.facebook.presto.hive.HiveSessionProperties.RESPECT_TABLE_FORMAT;
import static com.facebook.presto.hive.HiveSessionProperties.getBucketFunctionTypeForExchange;
import static com.facebook.presto.hive.HiveSessionProperties.getCompressionCodec;
import static com.facebook.presto.hive.HiveSessionProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveSessionProperties.getInsertExistingPartitionsBehavior;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcCompressionCodec;
import static com.facebook.presto.hive.HiveSessionProperties.getTemporaryTableCompressionCodec;
import static com.facebook.presto.hive.HiveSessionProperties.getTemporaryTableSchema;
import static com.facebook.presto.hive.HiveSessionProperties.getTemporaryTableStorageFormat;
import static com.facebook.presto.hive.HiveSessionProperties.getVirtualBucketCount;
import static com.facebook.presto.hive.HiveSessionProperties.isBucketExecutionEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isCollectColumnStatisticsOnWrite;
import static com.facebook.presto.hive.HiveSessionProperties.isCreateEmptyBucketFiles;
import static com.facebook.presto.hive.HiveSessionProperties.isFileRenamingEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isOfflineDataDebugModeEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isOptimizedMismatchedBucketCount;
import static com.facebook.presto.hive.HiveSessionProperties.isOptimizedPartitionUpdateSerializationEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isOrderBasedExecutionEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetPushdownFilterEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isPreferManifestsToListFiles;
import static com.facebook.presto.hive.HiveSessionProperties.isRespectTableFormat;
import static com.facebook.presto.hive.HiveSessionProperties.isShufflePartitionedColumnsForTableWriteEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isSortedWriteToTempPathEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isSortedWritingEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isStatisticsEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUsePageFileForHiveUnsupportedType;
import static com.facebook.presto.hive.HiveSessionProperties.shouldCreateEmptyBucketFilesForTemporaryTable;
import static com.facebook.presto.hive.HiveStatisticsUtil.createPartitionStatistics;
import static com.facebook.presto.hive.HiveStorageFormat.AVRO;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.values;
import static com.facebook.presto.hive.HiveTableProperties.AVRO_SCHEMA_URL;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.CSV_ESCAPE;
import static com.facebook.presto.hive.HiveTableProperties.CSV_QUOTE;
import static com.facebook.presto.hive.HiveTableProperties.CSV_SEPARATOR;
import static com.facebook.presto.hive.HiveTableProperties.DWRF_ENCRYPTION_ALGORITHM;
import static com.facebook.presto.hive.HiveTableProperties.DWRF_ENCRYPTION_PROVIDER;
import static com.facebook.presto.hive.HiveTableProperties.ENCRYPT_COLUMNS;
import static com.facebook.presto.hive.HiveTableProperties.ENCRYPT_TABLE;
import static com.facebook.presto.hive.HiveTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static com.facebook.presto.hive.HiveTableProperties.ORC_BLOOM_FILTER_FPP;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PREFERRED_ORDERING_COLUMNS;
import static com.facebook.presto.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.getAvroSchemaUrl;
import static com.facebook.presto.hive.HiveTableProperties.getBucketProperty;
import static com.facebook.presto.hive.HiveTableProperties.getCsvProperty;
import static com.facebook.presto.hive.HiveTableProperties.getDwrfEncryptionAlgorithm;
import static com.facebook.presto.hive.HiveTableProperties.getDwrfEncryptionProvider;
import static com.facebook.presto.hive.HiveTableProperties.getEncryptColumns;
import static com.facebook.presto.hive.HiveTableProperties.getEncryptTable;
import static com.facebook.presto.hive.HiveTableProperties.getExternalLocation;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveTableProperties.getOrcBloomFilterColumns;
import static com.facebook.presto.hive.HiveTableProperties.getOrcBloomFilterFpp;
import static com.facebook.presto.hive.HiveTableProperties.getPartitionedBy;
import static com.facebook.presto.hive.HiveTableProperties.getPreferredOrderingColumns;
import static com.facebook.presto.hive.HiveTableProperties.isExternalTable;
import static com.facebook.presto.hive.HiveType.HIVE_BINARY;
import static com.facebook.presto.hive.HiveType.toHiveType;
import static com.facebook.presto.hive.HiveUtil.columnExtraInfo;
import static com.facebook.presto.hive.HiveUtil.decodeMaterializedViewData;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.deserializeZstdCompressed;
import static com.facebook.presto.hive.HiveUtil.encodeMaterializedViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.HiveUtil.translateHiveUnsupportedTypeForTemporaryTable;
import static com.facebook.presto.hive.HiveUtil.translateHiveUnsupportedTypesForTemporaryTable;
import static com.facebook.presto.hive.HiveUtil.verifyPartitionTypeSupported;
import static com.facebook.presto.hive.HiveWriteUtils.isFileCreatedByQuery;
import static com.facebook.presto.hive.HiveWriteUtils.isWritableType;
import static com.facebook.presto.hive.HiveWriterFactory.computeBucketedFileName;
import static com.facebook.presto.hive.HiveWriterFactory.getFileExtension;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static com.facebook.presto.hive.MetadataUtils.createPredicate;
import static com.facebook.presto.hive.MetadataUtils.getCombinedRemainingPredicate;
import static com.facebook.presto.hive.MetadataUtils.getDiscretePredicates;
import static com.facebook.presto.hive.MetadataUtils.getPredicate;
import static com.facebook.presto.hive.MetadataUtils.getSubfieldPredicate;
import static com.facebook.presto.hive.MetadataUtils.isEntireColumn;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.APPEND;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.NEW;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.OVERWRITE;
import static com.facebook.presto.hive.SchemaProperties.getDatabaseProperties;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.toHivePrivilege;
import static com.facebook.presto.hive.metastore.MetastoreUtil.AVRO_SCHEMA_URL_KEY;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_MATERIALIZED_VIEW_FLAG;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_QUERY_ID_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VERSION_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.TABLE_COMMENT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static com.facebook.presto.hive.metastore.MetastoreUtil.checkIfNullView;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createTableObjectForViewCreation;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createViewProperties;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getPartitionNames;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getPartitionNamesWithEmptyVersion;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getProtectMode;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isDeltaLakeTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isIcebergTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isPrestoView;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isUserDefinedTypeEncodingEnabled;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyAndPopulateViews;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyOnline;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MATERIALIZED_VIEW;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.VIRTUAL_VIEW;
import static com.facebook.presto.hive.metastore.Statistics.ReduceOperator.ADD;
import static com.facebook.presto.hive.metastore.Statistics.createComputedStatisticsToPartitionMap;
import static com.facebook.presto.hive.metastore.Statistics.createEmptyPartitionStatistics;
import static com.facebook.presto.hive.metastore.Statistics.reduce;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.listEnabledPrincipals;
import static com.facebook.presto.hive.security.SqlStandardAccessControl.ADMIN_ROLE_NAME;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.FULLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.NOT_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.PARTIALLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.TOO_MANY_PARTITIONS_MISSING;
import static com.facebook.presto.spi.PartitionedTableWritePolicy.MULTIPLE_WRITERS_PER_PARTITION_ALLOWED;
import static com.facebook.presto.spi.PartitionedTableWritePolicy.SINGLE_WRITER_PER_PARTITION_REQUIRED;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.facebook.presto.spi.TableLayoutFilterCoverage.COVERED;
import static com.facebook.presto.spi.TableLayoutFilterCoverage.NOT_APPLICABLE;
import static com.facebook.presto.spi.TableLayoutFilterCoverage.NOT_COVERED;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;

public class HiveMetadata
        implements TransactionalMetadata
{
    private static final Logger log = Logger.get(HiveMetadata.class);
    public static final Set<String> RESERVED_ROLES = ImmutableSet.of("all", "default", "none");
    public static final String REFERENCED_MATERIALIZED_VIEWS = "referenced_materialized_views";

    private static final String ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    private static final String ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";

    private static final String PRESTO_TEMPORARY_TABLE_NAME_PREFIX = "__presto_temporary_table_";

    // Comma is not a reserved keyword with or without quote
    // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Keywords,Non-reservedKeywordsandReservedKeywords
    private static final char COMMA = ',';

    // 1009 is chosen as a prime number greater than 1000.
    // This is because Hive bucket function can result in skewed distribution when bucket number of power of 2
    // TODO: Use a regular number once better hash function is used for table write shuffle partitioning.
    private static final int SHUFFLE_MAX_PARALLELISM_FOR_PARTITIONED_TABLE_WRITE = 1009;

    private static final String CSV_SEPARATOR_KEY = OpenCSVSerde.SEPARATORCHAR;
    private static final String CSV_QUOTE_KEY = OpenCSVSerde.QUOTECHAR;
    private static final String CSV_ESCAPE_KEY = OpenCSVSerde.ESCAPECHAR;

    private static final JsonCodec<MaterializedViewDefinition> MATERIALIZED_VIEW_JSON_CODEC = jsonCodec(MaterializedViewDefinition.class);

    private final boolean allowCorruptWritesForTesting;
    private final SemiTransactionalHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;
    private final FilterStatsCalculatorService filterStatsCalculatorService;
    private final TableParameterCodec tableParameterCodec;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final SmileCodec<PartitionUpdate> partitionUpdateSmileCodec;
    private final boolean createsOfNonManagedTablesEnabled;
    private final int maxPartitionBatchSize;
    private final TypeTranslator typeTranslator;
    private final String prestoVersion;
    private final HiveStatisticsProvider hiveStatisticsProvider;
    private final StagingFileCommitter stagingFileCommitter;
    private final ZeroRowFileCreator zeroRowFileCreator;
    private final PartitionObjectBuilder partitionObjectBuilder;
    private final HiveEncryptionInformationProvider encryptionInformationProvider;
    private final HivePartitionStats hivePartitionStats;
    private final TableWritabilityChecker tableWritabilityChecker;

    public HiveMetadata(
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            boolean allowCorruptWritesForTesting,
            boolean createsOfNonManagedTablesEnabled,
            int maxPartitionBatchSize,
            TypeManager typeManager,
            LocationService locationService,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            FilterStatsCalculatorService filterStatsCalculatorService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            SmileCodec<PartitionUpdate> partitionUpdateSmileCodec,
            TypeTranslator typeTranslator,
            String prestoVersion,
            HiveStatisticsProvider hiveStatisticsProvider,
            StagingFileCommitter stagingFileCommitter,
            ZeroRowFileCreator zeroRowFileCreator,
            PartitionObjectBuilder partitionObjectBuilder,
            HiveEncryptionInformationProvider encryptionInformationProvider,
            HivePartitionStats hivePartitionStats,
            TableWritabilityChecker tableWritabilityChecker)
    {
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.filterStatsCalculatorService = requireNonNull(filterStatsCalculatorService, "filterStatsCalculatorService is null");
        this.tableParameterCodec = requireNonNull(tableParameterCodec, "tableParameterCodec is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.partitionUpdateSmileCodec = requireNonNull(partitionUpdateSmileCodec, "partitionUpdateSmileCodec is null");
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.hiveStatisticsProvider = requireNonNull(hiveStatisticsProvider, "hiveStatisticsProvider is null");
        this.stagingFileCommitter = requireNonNull(stagingFileCommitter, "stagingFileCommitter is null");
        this.zeroRowFileCreator = requireNonNull(zeroRowFileCreator, "zeroRowFileCreator is null");
        this.partitionObjectBuilder = requireNonNull(partitionObjectBuilder, "partitionObjectBuilder is null");
        this.encryptionInformationProvider = requireNonNull(encryptionInformationProvider, "encryptionInformationProvider is null");
        this.hivePartitionStats = requireNonNull(hivePartitionStats, "hivePartitionStats is null");
        this.tableWritabilityChecker = requireNonNull(tableWritabilityChecker, "tableWritabilityChecker is null");
    }

    public SemiTransactionalHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        Optional<Database> database = metastore.getDatabase(getMetastoreContext(session), schemaName);
        return database.isPresent();
    }

    @Override
    public HiveStatisticsProvider getHiveStatisticsProvider()
    {
        return hiveStatisticsProvider;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases(getMetastoreContext(session));
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Optional<Database> database = metastore.getDatabase(metastoreContext, schemaName.getSchemaName());
        if (database.isPresent()) {
            return getDatabaseProperties(database.get());
        }
        throw new SchemaNotFoundException(schemaName.getSchemaName());
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Optional<Table> table = metastore.getTable(metastoreContext, tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            return null;
        }

        if (getSourceTableNameFromSystemTable(tableName).isPresent()) {
            // We must not allow system table due to how permissions are checked in SystemTableAwareAccessControl.checkCanSelectFromTable()
            throw new PrestoException(NOT_SUPPORTED, "Unexpected table present in Hive metastore: " + tableName);
        }

        if (!isOfflineDataDebugModeEnabled(session)) {
            verifyOnline(tableName, Optional.empty(), getProtectMode(table.get()), table.get().getParameters());
        }

        return new HiveTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        HiveTableHandle handle = getTableHandle(session, tableName);
        if (handle == null) {
            return null;
        }
        Optional<List<List<String>>> partitionValuesList = getPartitionList(analyzeProperties);
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, handle);
        handle = handle.withAnalyzePartitionValues(partitionValuesList);

        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        if (partitionValuesList.isPresent() && partitionedBy.isEmpty()) {
            throw new PrestoException(INVALID_ANALYZE_PROPERTY, "Only partitioned table can be analyzed with a partition list");
        }
        return handle;
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (SystemTableHandler.PARTITIONS.matches(tableName)) {
            return getPartitionsSystemTable(session, tableName, SystemTableHandler.PARTITIONS.getSourceTableName(tableName));
        }
        if (SystemTableHandler.PROPERTIES.matches(tableName)) {
            return getPropertiesSystemTable(session, tableName, SystemTableHandler.PROPERTIES.getSourceTableName(tableName));
        }
        return Optional.empty();
    }

    private Optional<SystemTable> getPropertiesSystemTable(ConnectorSession session, SchemaTableName tableName, SchemaTableName sourceTableName)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Optional<Table> table = metastore.getTable(metastoreContext, sourceTableName.getSchemaName(), sourceTableName.getTableName());
        if (!table.isPresent() || table.get().getTableType().equals(VIRTUAL_VIEW)) {
            throw new TableNotFoundException(tableName);
        }
        Map<String, String> sortedTableParameters = ImmutableSortedMap.copyOf(table.get().getParameters());
        List<ColumnMetadata> columns = sortedTableParameters.keySet().stream()
                .map(key -> ColumnMetadata.builder().setName(normalizeIdentifier(session, key)).setType(VARCHAR).build())
                .collect(toImmutableList());
        List<Type> types = columns.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        Iterable<List<Object>> propertyValues = ImmutableList.of(ImmutableList.copyOf(sortedTableParameters.values()));

        return Optional.of(createSystemTable(new ConnectorTableMetadata(sourceTableName, columns), constraint -> new InMemoryRecordSet(types, propertyValues).cursor()));
    }

    private Optional<SystemTable> getPartitionsSystemTable(ConnectorSession session, SchemaTableName tableName, SchemaTableName sourceTableName)
    {
        HiveTableHandle sourceTableHandle = getTableHandle(session, sourceTableName);

        if (sourceTableHandle == null) {
            return Optional.empty();
        }

        MetastoreContext metastoreContext = getMetastoreContext(session);
        Table sourceTable = metastore.getTable(metastoreContext, sourceTableName.getSchemaName(), sourceTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(sourceTableName));
        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(sourceTable);
        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }

        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(HiveColumnHandle::getTypeSignature)
                .map(typeManager::getType)
                .collect(toImmutableList());

        List<ColumnMetadata> partitionSystemTableColumns = partitionColumns.stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(normalizeIdentifier(session, column.getName()))
                        .setType(typeManager.getType(column.getTypeSignature()))
                        .setComment(column.getComment().orElse(null))
                        .setHidden(column.isHidden())
                        .build())
                .collect(toImmutableList());

        Map<Integer, HiveColumnHandle> fieldIdToColumnHandle =
                IntStream.range(0, partitionColumns.size())
                        .boxed()
                        .collect(toImmutableMap(identity(), partitionColumns::get));

        return Optional.of(createSystemTable(
                new ConnectorTableMetadata(tableName, partitionSystemTableColumns),
                constraint -> {
                    TupleDomain<ColumnHandle> targetTupleDomain = constraint.transform(fieldIdToColumnHandle::get);
                    Predicate<Map<ColumnHandle, NullableValue>> targetPredicate = convertToPredicate(targetTupleDomain);
                    Constraint targetConstraint = new Constraint(targetTupleDomain, targetPredicate);
                    Iterable<List<Object>> records = () ->
                            partitionManager.getPartitionsList(metastore, sourceTableHandle, targetConstraint, session).stream()
                                    .map(hivePartition ->
                                            IntStream.range(0, partitionColumns.size())
                                                    .mapToObj(fieldIdToColumnHandle::get)
                                                    .map(columnHandle -> ((HivePartition) hivePartition).getKeys().get(columnHandle).getValue())
                                                    .collect(toList()))
                                    .iterator();

                    return new InMemoryRecordSet(partitionColumnTypes, records).cursor();
                }));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        Optional<Table> table = metastore.getTable(metastoreContext, hiveTableHandle);
        return getTableMetadata(table, hiveTableHandle.getSchemaTableName(), metastoreContext, session);
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Optional<Table> table = metastore.getTable(metastoreContext, tableName.getSchemaName(), tableName.getTableName());
        return getTableMetadata(table, tableName, metastoreContext, session);
    }

    private ConnectorTableMetadata getTableMetadata(Optional<Table> table, SchemaTableName tableName, MetastoreContext metastoreContext, ConnectorSession session)
    {
        if (!table.isPresent() || table.get().getTableType().equals(VIRTUAL_VIEW)) {
            throw new TableNotFoundException(tableName);
        }

        if (isIcebergTable(table.get()) || isDeltaLakeTable(table.get())) {
            throw new UnknownTableTypeException("Not a Hive table: " + tableName);
        }

        List<TableConstraint<String>> tableConstraints = metastore.getTableConstraints(metastoreContext, tableName.getSchemaName(), tableName.getTableName());
        List<String> notNullColumns = tableConstraints.stream()
                .filter(NotNullConstraint.class::isInstance)
                .map(constraint -> constraint.getColumns().stream()
                        .findFirst()
                        .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, format("NOT NULL constraint found with no column in table %s", tableName.getTableName()))))
                .collect(toImmutableList());
        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table.get(), typeManager, metastoreContext.getColumnConverter(), notNullColumns);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        Map<String, ColumnHandle> columnNameToHandleAssignments = new HashMap<>();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(table.get())) {
            columns.add(metadataGetter.apply(columnHandle));
            columnNameToHandleAssignments.put(columnHandle.getName(), columnHandle);
        }

        // External location property
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (table.get().getTableType().equals(EXTERNAL_TABLE)) {
            properties.put(EXTERNAL_LOCATION_PROPERTY, table.get().getStorage().getLocation());
        }

        // Storage format property
        HiveStorageFormat format = null;
        try {
            format = extractHiveStorageFormat(table.get());
            properties.put(STORAGE_FORMAT_PROPERTY, format);
        }
        catch (PrestoException ignored) {
            // todo fail if format is not known
        }

        getTableEncryptionPropertiesFromHiveProperties(table.get().getParameters(), format).map(TableEncryptionProperties::toTableProperties).ifPresent(properties::putAll);

        // Partitioning property
        List<String> partitionedBy = table.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        // Bucket properties
        Optional<HiveBucketProperty> bucketProperty = table.get().getStorage().getBucketProperty();
        table.get().getStorage().getBucketProperty().ifPresent(property -> {
            properties.put(BUCKET_COUNT_PROPERTY, property.getBucketCount());
            properties.put(BUCKETED_BY_PROPERTY, property.getBucketedBy());
            properties.put(SORTED_BY_PROPERTY, property.getSortedBy());
        });

        // Preferred ordering columns
        List<SortingColumn> preferredOrderingColumns = decodePreferredOrderingColumnsFromStorage(table.get().getStorage());
        if (!preferredOrderingColumns.isEmpty()) {
            if (bucketProperty.isPresent()) {
                throw new PrestoException(HIVE_INVALID_METADATA, format("bucketed table %s should not specify preferred_ordering_columns", tableName));
            }
            properties.put(PREFERRED_ORDERING_COLUMNS, preferredOrderingColumns);
        }

        // ORC format specific properties
        String orcBloomFilterColumns = table.get().getParameters().get(ORC_BLOOM_FILTER_COLUMNS_KEY);
        if (orcBloomFilterColumns != null) {
            properties.put(ORC_BLOOM_FILTER_COLUMNS, Splitter.on(COMMA).trimResults().omitEmptyStrings().splitToList(orcBloomFilterColumns));
        }
        String orcBloomFilterFfp = table.get().getParameters().get(ORC_BLOOM_FILTER_FPP_KEY);
        if (orcBloomFilterFfp != null) {
            properties.put(ORC_BLOOM_FILTER_FPP, Double.parseDouble(orcBloomFilterFfp));
        }

        // Avro specific property
        String avroSchemaUrl = table.get().getParameters().get(AVRO_SCHEMA_URL_KEY);
        if (avroSchemaUrl != null) {
            properties.put(AVRO_SCHEMA_URL, avroSchemaUrl);
        }

        // CSV specific property
        getCsvSerdeProperty(table.get(), CSV_SEPARATOR_KEY)
                .ifPresent(csvSeparator -> properties.put(CSV_SEPARATOR, csvSeparator));
        getCsvSerdeProperty(table.get(), CSV_QUOTE_KEY)
                .ifPresent(csvQuote -> properties.put(CSV_QUOTE, csvQuote));
        getCsvSerdeProperty(table.get(), CSV_ESCAPE_KEY)
                .ifPresent(csvEscape -> properties.put(CSV_ESCAPE, csvEscape));

        // Hook point for extended versions of the Hive Plugin
        properties.putAll(tableParameterCodec.decode(table.get().getParameters()));

        Optional<String> comment = Optional.ofNullable(table.get().getParameters().get(TABLE_COMMENT));

        return new ConnectorTableMetadata(tableName, columns.build(), properties.build(), comment, tableConstraints, columnNameToHandleAssignments);
    }

    private static Optional<String> getCsvSerdeProperty(Table table, String key)
    {
        return getSerdeProperty(table, key).map(csvSerdeProperty -> {
            if (csvSerdeProperty.length() > 1) {
                throw new PrestoException(HIVE_INVALID_METADATA, "Only single character can be set for property: " + key);
            }
            return csvSerdeProperty;
        });
    }

    private static Optional<String> getSerdeProperty(Table table, String key)
    {
        String serdePropertyValue = table.getStorage().getSerdeParameters().get(key);
        String tablePropertyValue = table.getParameters().get(key);
        if (serdePropertyValue != null && tablePropertyValue != null && !tablePropertyValue.equals(serdePropertyValue)) {
            // in Hive one can set conflicting values for the same property, in such case it looks like table properties are used
            throw new PrestoException(
                    HIVE_INVALID_METADATA,
                    format("Different values for '%s' set in serde properties and table properties: '%s' and '%s'", key, serdePropertyValue, tablePropertyValue));
        }
        return firstNonNullable(tablePropertyValue, serdePropertyValue);
    }

    protected Optional<? extends TableEncryptionProperties> getTableEncryptionPropertiesFromHiveProperties(Map<String, String> parameters, HiveStorageFormat storageFormat)
    {
        if (storageFormat != DWRF) {
            return Optional.empty();
        }

        return fromHiveTableProperties(parameters);
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        HiveTableLayoutHandle tableLayoutHandle = (HiveTableLayoutHandle) layoutHandle;
        if (tableLayoutHandle.getPartitions().isPresent()) {
            return Optional.of(new HiveInputInfo(
                    tableLayoutHandle.getPartitions().get().stream()
                            .map(hivePartition -> hivePartition.getPartitionId().getPartitionName())
                            .collect(toList()),
                    false, tableLayoutHandle.getTablePath()));
        }

        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        MetastoreContext metastoreContext = getMetastoreContext(session);
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : metastore.getAllTables(metastoreContext, schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Table table = metastore.getTable(metastoreContext, hiveTableHandle)
                .orElseThrow(() -> new TableNotFoundException(hiveTableHandle.getSchemaTableName()));
        return hiveColumnHandles(table).stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
    }

    @SuppressWarnings("TryWithIdenticalCatches")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (HiveViewNotSupportedException e) {
                // view is not supported
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
            catch (UnknownTableTypeException e) {
                log.warn(String.format("%s: Unknown table type of table %s", e.getMessage(), tableName));
            }
        }
        return columns.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }

        // TODO Adjust statistics to take into account required subfields

        if (!tableLayoutHandle.isPresent() || !((HiveTableLayoutHandle) tableLayoutHandle.get()).isPushdownFilterEnabled()) {
            Map<String, ColumnHandle> columns = columnHandles.stream()
                    .map(HiveColumnHandle.class::cast)
                    .filter(not(HiveColumnHandle::isHidden))
                    .collect(toImmutableMap(HiveColumnHandle::getName, Function.identity()));

            Map<String, Type> columnTypes = columns.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> getColumnMetadata(session, tableHandle, entry.getValue()).getType()));
            List<HivePartition> partitions = partitionManager.getPartitions(metastore, tableHandle, constraint, session).getPartitions();
            return hiveStatisticsProvider.getTableStatistics(session, ((HiveTableHandle) tableHandle).getSchemaTableName(), columns, columnTypes, partitions);
        }

        verify(!constraint.predicate().isPresent());

        HiveTableLayoutHandle hiveLayoutHandle = (HiveTableLayoutHandle) tableLayoutHandle.get();

        Set<String> columnNames = columnHandles.stream()
                .map(HiveColumnHandle.class::cast)
                .map(HiveColumnHandle::getName)
                .collect(toImmutableSet());

        Set<ColumnHandle> allColumnHandles = ImmutableSet.<ColumnHandle>builder()
                .addAll(columnHandles)
                .addAll(hiveLayoutHandle.getPredicateColumns().values().stream()
                        .filter(column -> !columnNames.contains(column.getName()))
                        .collect(toImmutableList()))
                .build();

        Map<String, ColumnHandle> allColumns = Maps.uniqueIndex(allColumnHandles, column -> ((HiveColumnHandle) column).getName());

        Map<String, Type> allColumnTypes = allColumns.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> getColumnMetadata(session, tableHandle, entry.getValue()).getType()));

        Constraint<ColumnHandle> combinedConstraint = new Constraint<>(constraint.getSummary().intersect(hiveLayoutHandle.getDomainPredicate()
                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                .transform(allColumns::get)));

        SubfieldExtractor subfieldExtractor = new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(session), session);

        RowExpression domainPredicate = rowExpressionService.getDomainTranslator().toPredicate(
                hiveLayoutHandle.getDomainPredicate()
                        .transform(subfield -> subfieldExtractor.toRowExpression(subfield, allColumnTypes.get(subfield.getRootName()))));
        RowExpression combinedPredicate = binaryExpression(SpecialFormExpression.Form.AND, ImmutableList.of(hiveLayoutHandle.getRemainingPredicate(), domainPredicate));

        List<HivePartition> partitions = partitionManager.getPartitions(metastore, tableHandle, combinedConstraint, session).getPartitions();
        TableStatistics tableStatistics = hiveStatisticsProvider.getTableStatistics(session, ((HiveTableHandle) tableHandle).getSchemaTableName(), allColumns, allColumnTypes, partitions);

        return filterStatsCalculatorService.filterStats(tableStatistics, combinedPredicate, session, ImmutableBiMap.copyOf(allColumns).inverse(), allColumnTypes);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    /**
     * NOTE: This method does not return column comment
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getColumnMetadata(typeManager);
    }

    /**
     * Returns a TupleDomain of constraints that is suitable for Explain (Type IO)
     * <p>
     * Only Hive partition columns that are used in IO planning.
     */
    @Override
    public TupleDomain<ColumnHandle> toExplainIOConstraints(ConnectorSession session, ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> constraints)
    {
        return constraints.transform(columnHandle -> ((HiveColumnHandle) columnHandle).isPartitionKey() ? columnHandle : null);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Optional<String> location = SchemaProperties.getLocation(properties).map(locationUri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(locationUri));
            }
            catch (IOException e) {
                throw new PrestoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + locationUri, e);
            }
            return locationUri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(USER)
                .setOwnerName(session.getUser())
                .build();

        metastore.createDatabase(getMetastoreContext(session), database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, schemaName).isEmpty() ||
                !listViews(session, schemaName).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        metastore.dropDatabase(getMetastoreContext(session), schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(getMetastoreContext(session), source, target);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        PrestoTableType tableType = isExternalTable(tableMetadata.getProperties()) ? EXTERNAL_TABLE : MANAGED_TABLE;
        Table table = prepareTable(session, tableMetadata, tableType);
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner());
        HiveBasicStatistics basicStatistics = table.getPartitionColumns().isEmpty() ? createZeroStatistics() : createEmptyStatistics();

        List<TableConstraint<String>> constraints = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.isNullable())
                .map(columnMetadata -> new NotNullConstraint<>(columnMetadata.getName()))
                .collect(Collectors.toList());
        constraints.addAll(tableMetadata.getTableConstraintsHolder().getTableConstraints());

        metastore.createTable(
                session,
                table,
                principalPrivileges,
                Optional.empty(),
                ignoreExisting,
                new PartitionStatistics(basicStatistics, ImmutableMap.of()),
                constraints);
    }

    private Table prepareTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, PrestoTableType tableType)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());

        if ((bucketProperty.isPresent() || !partitionedBy.isEmpty()) && getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new PrestoException(NOT_SUPPORTED, "Bucketing/Partitioning columns not supported when Avro schema url is set");
        }

        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator);
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        List<SortingColumn> preferredOrderingColumns = getPreferredOrderingColumns(tableMetadata.getProperties());

        Optional<TableEncryptionProperties> tableEncryptionProperties = getTableEncryptionPropertiesFromTableProperties(tableMetadata, hiveStorageFormat, partitionedBy);

        if (tableEncryptionProperties.isPresent() && partitionedBy.isEmpty()) {
            throw new PrestoException(HIVE_UNSUPPORTED_ENCRYPTION_OPERATION, "Creating an encrypted table without partitions is not supported. Use CREATE TABLE AS SELECT to " +
                    "create an encrypted table without partitions");
        }

        validateColumns(hiveStorageFormat, columnHandles);

        MetastoreContext metastoreContext = getMetastoreContext(session);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(columnHandle -> columnHandleToColumn(metastoreContext, columnHandle))
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        Path targetPath;
        if (tableType.equals(EXTERNAL_TABLE)) {
            if (!createsOfNonManagedTablesEnabled) {
                throw new PrestoException(NOT_SUPPORTED, "Cannot create non-managed Hive table");
            }
            String externalLocation = getExternalLocation(tableMetadata.getProperties());
            targetPath = getExternalPath(new HdfsContext(session, schemaName, tableName, externalLocation, true), externalLocation);
        }
        else if (tableType.equals(MANAGED_TABLE) || tableType.equals(MATERIALIZED_VIEW)) {
            LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, isTempPathRequired(session, bucketProperty, preferredOrderingColumns));
            targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
        }
        else {
            throw new IllegalStateException(format("%s is not a valid table type to be created.", tableType));
        }

        Map<String, String> tableProperties = getEmptyTableProperties(
                tableMetadata,
                new HdfsContext(session, schemaName, tableName, targetPath.toString(), true),
                hiveStorageFormat,
                tableEncryptionProperties);

        return buildTableObject(
                session.getQueryId(),
                schemaName,
                tableName,
                session.getUser(),
                columnHandles,
                hiveStorageFormat,
                partitionedBy,
                bucketProperty,
                preferredOrderingColumns,
                tableProperties,
                targetPath,
                tableType,
                prestoVersion,
                metastoreContext);
    }

    @Override
    public ConnectorTableHandle createTemporaryTable(ConnectorSession session, List<ColumnMetadata> columns, Optional<ConnectorPartitioningMetadata> partitioningMetadata)
    {
        String schemaName = getTemporaryTableSchema(session);
        HiveStorageFormat storageFormat = getTemporaryTableStorageFormat(session);

        Optional<HiveBucketProperty> bucketProperty = partitioningMetadata.map(partitioning -> {
            Set<String> allColumns = columns.stream()
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableSet());
            if (!allColumns.containsAll(partitioning.getPartitionColumns())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format(
                        "Bucketing columns %s not present in schema",
                        Sets.difference(ImmutableSet.copyOf(partitioning.getPartitionColumns()), allColumns)));
            }
            HivePartitioningHandle partitioningHandle = (HivePartitioningHandle) partitioning.getPartitioningHandle();
            List<String> partitionColumns = partitioning.getPartitionColumns();
            BucketFunctionType bucketFunctionType = partitioningHandle.getBucketFunctionType();
            switch (bucketFunctionType) {
                case HIVE_COMPATIBLE:
                    return new HiveBucketProperty(
                            partitionColumns,
                            partitioningHandle.getBucketCount(),
                            ImmutableList.of(),
                            HIVE_COMPATIBLE,
                            Optional.empty());
                case PRESTO_NATIVE:
                    Map<String, Type> columnNameToTypeMap = columns.stream()
                            .collect(toMap(ColumnMetadata::getName, ColumnMetadata::getType));
                    return new HiveBucketProperty(
                            partitionColumns,
                            partitioningHandle.getBucketCount(),
                            ImmutableList.of(),
                            PRESTO_NATIVE,
                            Optional.of(partitionColumns.stream()
                                    .map(columnNameToTypeMap::get)
                                    .collect(toImmutableList())));
                default:
                    throw new IllegalArgumentException("Unsupported bucket function type " + bucketFunctionType);
            }
        });

        if (isUsePageFileForHiveUnsupportedType(session)) {
            if (!columns.stream()
                    .map(ColumnMetadata::getType)
                    .allMatch(HiveTypeTranslator::isSupportedHiveType)) {
                storageFormat = PAGEFILE;
            }
        }

        // PAGEFILE format doesn't require translation to hive type,
        // choose HIVE_BINARY as a default hive type to make it compatible with Hive connector
        Optional<HiveType> defaultHiveType = storageFormat == PAGEFILE ? Optional.of(HIVE_BINARY) : Optional.empty();

        List<HiveColumnHandle> columnHandles = getColumnHandles(
                // Hive doesn't support anonymous rows and unknown type
                // Since this method doesn't create a real table, it is fine
                // to assign dummy field names to the anonymous rows and translate unknown
                // type to the boolean type that is binary compatible
                translateHiveUnsupportedTypesForTemporaryTable(columns, typeManager),
                ImmutableSet.of(),
                typeTranslator,
                defaultHiveType);

        validateColumns(storageFormat, columnHandles);

        HiveStorageFormat finalStorageFormat = storageFormat;
        String tableName = PRESTO_TEMPORARY_TABLE_NAME_PREFIX + finalStorageFormat.name() +
                "_" + session.getQueryId().replaceAll("-", "_") + "_" + randomUUID().toString().replaceAll("-", "_");
        Table table = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(session.getUser())
                .setTableType(TEMPORARY_TABLE)
                .setDataColumns(columnHandles.stream()
                        // Not propagating column type metadata because temp tables are not visible to users
                        .map(handle -> new Column(handle.getName(), handle.getHiveType(), handle.getComment(), Optional.empty()))
                        .collect(toImmutableList()))
                .withStorage(storage -> storage
                        .setStorageFormat(fromHiveStorageFormat(finalStorageFormat))
                        .setBucketProperty(bucketProperty)
                        .setLocation(""))
                .build();

        List<String> partitionColumnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        List<HiveColumnHandle> hiveColumnHandles = hiveColumnHandles(table);
        Map<String, Type> columnTypes = hiveColumnHandles.stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));
        Map<String, Set<ColumnStatisticType>> columnStatisticTypes = hiveColumnHandles.stream()
                .filter(columnHandle -> !partitionColumnNames.contains(columnHandle.getName()))
                .filter(column -> !column.isHidden())
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> ImmutableSet.copyOf(getSupportedColumnStatisticsForTemporaryTable(typeManager.getType(column.getTypeSignature())))));

        metastore.createTable(
                session,
                table,
                buildInitialPrivilegeSet(table.getOwner()),
                Optional.empty(),
                false,
                createEmptyPartitionStatistics(columnTypes, columnStatisticTypes),
                emptyList());

        return new HiveTableHandle(schemaName, tableName);
    }

    private Set<ColumnStatisticType> getSupportedColumnStatisticsForTemporaryTable(Type type)
    {
        // Temporary table statistics are not committed to metastore, so no need to call metastore for supported
        // column statistics, instead locally determine.
        if (type.equals(BOOLEAN)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_TRUE_VALUES);
        }
        if (isNumericType(type) || type.equals(DATE) || type.equals(TIMESTAMP)) {
            return ImmutableSet.of(MIN_VALUE, MAX_VALUE, NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES);
        }
        if (isVarcharType(type) || isCharType(type)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type.equals(VARBINARY)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type instanceof ArrayType || type instanceof RowType || type instanceof MapType) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES);
        }
        // Default column statistics for unknown data types.
        return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES);
    }

    private void validateColumns(HiveStorageFormat hiveStorageFormat, List<HiveColumnHandle> handles)
    {
        if (hiveStorageFormat == AVRO) {
            for (HiveColumnHandle handle : handles) {
                if (!handle.isPartitionKey()) {
                    validateAvroType(handle.getHiveType().getTypeInfo(), handle.getName());
                }
            }
        }
    }

    private static void validateAvroType(TypeInfo type, String columnName)
    {
        if (type.getCategory() == ObjectInspector.Category.MAP) {
            TypeInfo keyType = mapTypeInfo(type).getMapKeyTypeInfo();
            if ((keyType.getCategory() != ObjectInspector.Category.PRIMITIVE) ||
                    (primitiveTypeInfo(keyType).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING)) {
                throw new PrestoException(NOT_SUPPORTED, format("Column %s has a non-varchar map key, which is not supported by Avro", columnName));
            }
        }
        else if (type.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            PrimitiveObjectInspector.PrimitiveCategory primitive = primitiveTypeInfo(type).getPrimitiveCategory();
            if (primitive == PrimitiveObjectInspector.PrimitiveCategory.BYTE) {
                throw new PrestoException(NOT_SUPPORTED, format("Column %s is tinyint, which is not supported by Avro. Use integer instead.", columnName));
            }
            if (primitive == PrimitiveObjectInspector.PrimitiveCategory.SHORT) {
                throw new PrestoException(NOT_SUPPORTED, format("Column %s is smallint, which is not supported by Avro. Use integer instead.", columnName));
            }
        }
    }

    private static PrimitiveTypeInfo primitiveTypeInfo(TypeInfo typeInfo)
    {
        return (PrimitiveTypeInfo) typeInfo;
    }

    private static MapTypeInfo mapTypeInfo(TypeInfo typeInfo)
    {
        return (MapTypeInfo) typeInfo;
    }

    private Map<String, String> getEmptyTableProperties(
            ConnectorTableMetadata tableMetadata,
            HdfsContext hdfsContext,
            HiveStorageFormat hiveStorageFormat,
            Optional<TableEncryptionProperties> tableEncryptionProperties)
    {
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();

        // Hook point for extended versions of the Hive Plugin
        tableProperties.putAll(tableParameterCodec.encode(tableMetadata.getProperties()));

        // ORC format specific properties
        List<String> columns = getOrcBloomFilterColumns(tableMetadata.getProperties());
        if (columns != null && !columns.isEmpty()) {
            tableProperties.put(ORC_BLOOM_FILTER_COLUMNS_KEY, Joiner.on(COMMA).join(columns));
            tableProperties.put(ORC_BLOOM_FILTER_FPP_KEY, String.valueOf(getOrcBloomFilterFpp(tableMetadata.getProperties())));
        }

        // Avro specific properties
        String avroSchemaUrl = getAvroSchemaUrl(tableMetadata.getProperties());
        if (avroSchemaUrl != null) {
            if (hiveStorageFormat != AVRO) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Cannot specify %s table property for storage format: %s", AVRO_SCHEMA_URL, hiveStorageFormat));
            }
            tableProperties.put(AVRO_SCHEMA_URL_KEY, validateAndNormalizeAvroSchemaUrl(avroSchemaUrl, hdfsContext));
        }

        // CSV specific properties
        getCsvProperty(tableMetadata.getProperties(), CSV_ESCAPE)
                .ifPresent(escape -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, CSV_ESCAPE);
                    tableProperties.put(CSV_ESCAPE_KEY, escape.toString());
                });
        getCsvProperty(tableMetadata.getProperties(), CSV_QUOTE)
                .ifPresent(quote -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, CSV_QUOTE);
                    tableProperties.put(CSV_QUOTE_KEY, quote.toString());
                });
        getCsvProperty(tableMetadata.getProperties(), CSV_SEPARATOR)
                .ifPresent(separator -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, CSV_SEPARATOR);
                    tableProperties.put(CSV_SEPARATOR_KEY, separator.toString());
                });

        // Table comment property
        tableMetadata.getComment().ifPresent(value -> tableProperties.put(TABLE_COMMENT, value));

        // Encryption specific settings
        tableProperties.putAll(tableEncryptionProperties.map(TableEncryptionProperties::toHiveProperties).orElseGet(ImmutableMap::of));

        return tableProperties.build();
    }

    private static void checkFormatForProperty(HiveStorageFormat actualStorageFormat, HiveStorageFormat expectedStorageFormat, String propertyName)
    {
        if (actualStorageFormat != expectedStorageFormat) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Cannot specify %s table property for storage format: %s", propertyName, actualStorageFormat));
        }
    }

    private String validateAndNormalizeAvroSchemaUrl(String url, HdfsContext context)
    {
        try {
            new URL(url).openStream().close();
            return url;
        }
        catch (MalformedURLException e) {
            // try locally
            if (new File(url).exists()) {
                // hive needs url to have a protocol
                return new File(url).toURI().toString();
            }
            // try hdfs
            try {
                if (!hdfsEnvironment.getFileSystem(context, new Path(url)).exists(new Path(url))) {
                    throw new PrestoException(INVALID_TABLE_PROPERTY, "Cannot locate Avro schema file: " + url);
                }
                return url;
            }
            catch (IOException ex) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Avro schema file is not a valid file system URI: " + url, ex);
            }
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Cannot open Avro schema file: " + url, e);
        }
    }

    private Path getExternalPath(HdfsContext context, String location)
    {
        try {
            Path path = new Path(location);
            if (!hdfsEnvironment.getFileSystem(context, path).isDirectory(path)) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "External location must be a directory");
            }
            return path;
        }
        catch (IllegalArgumentException | IOException e) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "External location is not a valid file system URI", e);
        }
    }

    private void checkPartitionTypesSupported(List<Column> partitionColumns)
    {
        for (Column partitionColumn : partitionColumns) {
            Type partitionType = typeManager.getType(partitionColumn.getType().getTypeSignature());
            verifyPartitionTypeSupported(partitionColumn.getName(), partitionType);
        }
    }

    private static Table buildTableObject(
            String queryId,
            String schemaName,
            String tableName,
            String tableOwner,
            List<HiveColumnHandle> columnHandles,
            HiveStorageFormat hiveStorageFormat,
            List<String> partitionedBy,
            Optional<HiveBucketProperty> bucketProperty,
            List<SortingColumn> preferredOrderingColumns,
            Map<String, String> additionalTableParameters,
            Path targetPath,
            PrestoTableType tableType,
            String prestoVersion,
            MetastoreContext metastoreContext)
    {
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> columnHandleToColumn(metastoreContext, column))
                .collect(toList());

        Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            HiveType type = columnHandle.getHiveType();
            if (!partitionColumnNames.contains(name)) {
                verify(!columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
                columns.add(columnHandleToColumn(metastoreContext, columnHandle));
            }
            else {
                verify(columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
            }
        }

        ImmutableMap.Builder<String, String> tableParameters = ImmutableMap.<String, String>builder()
                .put(PRESTO_VERSION_NAME, prestoVersion)
                .put(PRESTO_QUERY_ID_NAME, queryId)
                .putAll(additionalTableParameters);

        if (tableType.equals(EXTERNAL_TABLE)) {
            tableParameters.put("EXTERNAL", "TRUE");
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(tableOwner)
                .setTableType(tableType)
                .setDataColumns(columns.build())
                .setPartitionColumns(partitionColumns)
                .setParameters(tableParameters.build());

        tableBuilder.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(hiveStorageFormat))
                .setBucketProperty(bucketProperty)
                .setParameters(ImmutableMap.of(PREFERRED_ORDERING_COLUMNS, encodePreferredOrderingColumns(preferredOrderingColumns)))
                .setLocation(targetPath.toString());

        return tableBuilder.build();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, handle);
        if (!column.isNullable()) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support ADD COLUMN with NOT NULL");
        }

        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.addColumn(metastoreContext, handle.getSchemaName(), handle.getTableName(), column.getName(), toHiveType(typeTranslator, column.getType()), column.getComment().orElse(null));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, hiveTableHandle);
        HiveColumnHandle sourceHandle = (HiveColumnHandle) source;

        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.renameColumn(metastoreContext, hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), sourceHandle.getName(), target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, hiveTableHandle);
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;

        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.dropColumn(metastoreContext, hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), columnHandle.getName());
    }

    private void failIfAvroSchemaIsSet(ConnectorSession session, HiveTableHandle handle)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);

        Table table = metastore.getTable(metastoreContext, handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));

        if (table.getParameters().get(AVRO_SCHEMA_URL_KEY) != null) {
            throw new PrestoException(NOT_SUPPORTED, "ALTER TABLE not supported when Avro schema url is set");
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.renameTable(metastoreContext, handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        MetastoreContext metastoreContext = getMetastoreContext(session);

        Optional<Table> target = metastore.getTable(metastoreContext, handle);
        if (!target.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        metastore.dropTable(
                new HdfsContext(session, handle.getSchemaName(), handle.getTableName(), target.get().getStorage().getLocation(), false),
                handle.getSchemaName(),
                handle.getTableName());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        verifyJvmTimeZone();
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        MetastoreContext metastoreContext = getMetastoreContext(session);

        metastore.getTable(metastoreContext, hiveTableHandle)
                .orElseThrow(() -> new TableNotFoundException(tableName));
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = handle.getSchemaTableName();
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Table table = metastore.getTable(metastoreContext, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));

        List<Column> partitionColumns = table.getPartitionColumns();
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        List<HiveColumnHandle> hiveColumnHandles = hiveColumnHandles(table);
        Map<String, Type> columnTypes = hiveColumnHandles.stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));

        Map<List<String>, ComputedStatistics> computedStatisticsMap = createComputedStatisticsToPartitionMap(computedStatistics, partitionColumnNames, columnTypes);

        if (partitionColumns.isEmpty()) {
            // commit analyze to unpartitioned table
            metastore.setTableStatistics(metastoreContext, table, createPartitionStatistics(session, columnTypes, computedStatisticsMap.get(ImmutableList.<String>of()), timeZone));
        }
        else {
            List<String> partitionNames;
            List<List<String>> partitionValuesList;
            if (handle.getAnalyzePartitionValues().isPresent()) {
                partitionValuesList = handle.getAnalyzePartitionValues().get();
                partitionNames = partitionValuesList.stream()
                        .map(partitionValues -> makePartName(partitionColumns, partitionValues))
                        .collect(toImmutableList());
            }
            else {
                partitionNames = getPartitionNames(metastore.getPartitionNames(metastoreContext, handle.getSchemaName(), handle.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(tableName)));
                partitionValuesList = partitionNames
                        .stream()
                        .map(MetastoreUtil::toPartitionValues)
                        .collect(toImmutableList());
            }

            ImmutableMap.Builder<List<String>, PartitionStatistics> partitionStatistics = ImmutableMap.builder();
            Map<String, Set<ColumnStatisticType>> columnStatisticTypes = hiveColumnHandles.stream()
                    .filter(columnHandle -> !partitionColumnNames.contains(columnHandle.getName()))
                    .filter(column -> !column.isHidden())
                    .collect(toImmutableMap(HiveColumnHandle::getName, column -> ImmutableSet.copyOf(metastore.getSupportedColumnStatistics(metastoreContext, typeManager.getType(column.getTypeSignature())))));
            Supplier<PartitionStatistics> emptyPartitionStatistics = Suppliers.memoize(() -> createEmptyPartitionStatistics(columnTypes, columnStatisticTypes));

            int usedComputedStatistics = 0;
            List<String> partitionedBy = table.getPartitionColumns().stream()
                    .map(Column::getName)
                    .collect(toImmutableList());
            List<Type> partitionTypes = partitionedBy.stream()
                    .map(columnTypes::get)
                    .collect(toImmutableList());
            for (int i = 0; i < partitionNames.size(); i++) {
                String partitionName = partitionNames.get(i);
                List<String> partitionValues = partitionValuesList.get(i);
                ComputedStatistics collectedStatistics = computedStatisticsMap.containsKey(partitionValues)
                        ? computedStatisticsMap.get(partitionValues)
                        : computedStatisticsMap.get(canonicalizePartitionValues(partitionName, partitionValues, partitionTypes));
                if (collectedStatistics == null) {
                    partitionStatistics.put(partitionValues, emptyPartitionStatistics.get());
                }
                else {
                    usedComputedStatistics++;
                    partitionStatistics.put(partitionValues, createPartitionStatistics(session, columnTypes, collectedStatistics, timeZone));
                }
            }
            verify(usedComputedStatistics == computedStatistics.size(),
                    usedComputedStatistics > computedStatistics.size() ?
                            "There are multiple variants of the same partition, e.g. p=1, p=01, p=001. All partitions must follow the same key=value representation" :
                            "All computed statistics must be used");
            metastore.setPartitionStatistics(metastoreContext, table, partitionStatistics.build());
        }
    }

    private static Map<ColumnStatisticMetadata, Block> getColumnStatistics(Map<List<String>, ComputedStatistics> statistics, List<String> partitionValues)
    {
        return Optional.ofNullable(statistics.get(partitionValues))
                .map(ComputedStatistics::getColumnStatistics)
                .orElse(ImmutableMap.of());
    }

    private Map<ColumnStatisticMetadata, Block> getColumnStatistics(
            Map<List<String>, ComputedStatistics> statistics,
            String partitionName,
            List<String> partitionValues,
            List<Type> partitionTypes)
    {
        Optional<Map<ColumnStatisticMetadata, Block>> columnStatistics = Optional.ofNullable(statistics.get(partitionValues))
                .map(ComputedStatistics::getColumnStatistics);
        return columnStatistics
                .orElseGet(() -> getColumnStatistics(statistics, canonicalizePartitionValues(partitionName, partitionValues, partitionTypes)));
    }

    private List<String> canonicalizePartitionValues(String partitionName, List<String> partitionValues, List<Type> partitionTypes)
    {
        verify(partitionValues.size() == partitionTypes.size(), "Expected partitionTypes size to be %s but got %s", partitionValues.size(), partitionTypes.size());
        Block[] parsedPartitionValuesBlocks = new Block[partitionValues.size()];
        for (int i = 0; i < partitionValues.size(); i++) {
            String partitionValue = partitionValues.get(i);
            Type partitionType = partitionTypes.get(i);
            parsedPartitionValuesBlocks[i] = parsePartitionValue(partitionName, partitionValue, partitionType, timeZone).asBlock();
        }

        return createPartitionValues(partitionTypes, new Page(parsedPartitionValuesBlocks), 0);
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        verifyJvmTimeZone();

        if (getExternalLocation(tableMetadata.getProperties()) != null) {
            throw new PrestoException(NOT_SUPPORTED, "External tables cannot be created using CREATE TABLE AS");
        }

        if (getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new PrestoException(NOT_SUPPORTED, "CREATE TABLE AS not supported when Avro schema url is set");
        }

        HiveStorageFormat tableStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        List<SortingColumn> preferredOrderingColumns = getPreferredOrderingColumns(tableMetadata.getProperties());

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Optional<TableEncryptionProperties> tableEncryptionProperties = getTableEncryptionPropertiesFromTableProperties(tableMetadata, tableStorageFormat, partitionedBy);
        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator);
        HiveStorageFormat partitionStorageFormat = isRespectTableFormat(session) ? tableStorageFormat : getHiveStorageFormat(session);

        // unpartitioned tables ignore the partition storage format
        HiveStorageFormat actualStorageFormat = partitionedBy.isEmpty() ? tableStorageFormat : partitionStorageFormat;
        validateColumns(actualStorageFormat, columnHandles);

        if (tableEncryptionProperties.isPresent() && tableStorageFormat != actualStorageFormat) {
            throw new PrestoException(
                    INVALID_TABLE_PROPERTY,
                    format(
                            "For encrypted tables, partition format (%s) should match table format (%s). Using the session property %s or appropriately setting %s can help with ensuring this",
                            actualStorageFormat.name(),
                            tableStorageFormat.name(),
                            RESPECT_TABLE_FORMAT,
                            HIVE_STORAGE_FORMAT));
        }
        MetastoreContext metastoreContext = getMetastoreContext(session);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(columnHandle -> columnHandleToColumn(metastoreContext, columnHandle))
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, isTempPathRequired(session, bucketProperty, preferredOrderingColumns));

        HdfsContext context = new HdfsContext(session, schemaName, tableName, locationHandle.getTargetPath().toString(), true);
        Map<String, String> tableProperties = getEmptyTableProperties(
                tableMetadata,
                context,
                tableStorageFormat,
                tableEncryptionProperties);
        HiveOutputTableHandle result = new HiveOutputTableHandle(
                schemaName,
                tableName,
                columnHandles,
                metastore.generatePageSinkMetadata(metastoreContext, schemaTableName),
                locationHandle,
                tableStorageFormat,
                partitionStorageFormat,
                actualStorageFormat,
                getHiveCompressionCodec(session, false, actualStorageFormat),
                partitionedBy,
                bucketProperty,
                preferredOrderingColumns,
                session.getUser(),
                tableProperties,
                encryptionInformationProvider.getWriteEncryptionInformation(session, tableEncryptionProperties, schemaName, tableName));

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(
                context,
                metastoreContext,
                writeInfo.getWriteMode(),
                writeInfo.getWritePath(),
                writeInfo.getTempPath(),
                schemaTableName,
                false);

        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;

        List<PartitionUpdate> partitionUpdates = getPartitionUpdates(session, fragments);

        Map<String, String> tableEncryptionParameters = ImmutableMap.of();
        Map<String, String> partitionEncryptionParameters = ImmutableMap.of();
        if (handle.getEncryptionInformation().isPresent()) {
            EncryptionInformation encryptionInformation = handle.getEncryptionInformation().get();
            if (encryptionInformation.getDwrfEncryptionMetadata().isPresent()) {
                if (handle.getPartitionedBy().isEmpty()) {
                    tableEncryptionParameters = ImmutableMap.copyOf(encryptionInformation.getDwrfEncryptionMetadata().get().getExtraMetadata());
                }
                else {
                    partitionEncryptionParameters = ImmutableMap.copyOf(encryptionInformation.getDwrfEncryptionMetadata().get().getExtraMetadata());
                }
            }
        }

        WriteInfo writeInfo = locationService.getQueryWriteInfo(handle.getLocationHandle());
        MetastoreContext metastoreContext = getMetastoreContext(session);

        Table table = buildTableObject(
                session.getQueryId(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableOwner(),
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionedBy(),
                handle.getBucketProperty(),
                handle.getPreferredOrderingColumns(),
                ImmutableMap.<String, String>builder()
                        .putAll(handle.getAdditionalTableParameters())
                        .putAll(tableEncryptionParameters)
                        .build(),
                writeInfo.getTargetPath(),
                MANAGED_TABLE,
                prestoVersion,
                metastoreContext);
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(handle.getTableOwner());

        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        if (handle.getBucketProperty().isPresent() && isCreateEmptyBucketFiles(session)) {
            List<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(
                    session,
                    handle,
                    table,
                    partitionUpdates);
            // replace partitionUpdates before creating the zero-row files so that those files will be cleaned up if we roll back
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            HdfsContext hdfsContext = new HdfsContext(session, table.getDatabaseName(), table.getTableName(), table.getStorage().getLocation(), true);
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() :
                        Optional.of(partitionObjectBuilder.buildPartitionObject(session, table, partitionUpdate, prestoVersion, partitionEncryptionParameters, Optional.empty(), Optional.empty()));
                zeroRowFileCreator.createFiles(
                        session,
                        hdfsContext,
                        partitionUpdate.getWritePath(),
                        getTargetFileNames(partitionUpdate.getFileWriteInfos()),
                        getStorageFormat(partition, table),
                        handle.getCompressionCodec(),
                        getSchema(partition, table));
            }
        }

        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = createComputedStatisticsToPartitionMap(computedStatistics, handle.getPartitionedBy(), columnTypes);

        PartitionStatistics tableStatistics;
        if (table.getPartitionColumns().isEmpty()) {
            HiveBasicStatistics basicStatistics = partitionUpdates.stream()
                    .map(PartitionUpdate::getStatistics)
                    .reduce((first, second) -> reduce(first, second, ADD))
                    .orElseGet(() -> createZeroStatistics());
            tableStatistics = createPartitionStatistics(session, basicStatistics, columnTypes, getColumnStatistics(partitionComputedStatistics, ImmutableList.of()), timeZone);
        }
        else {
            tableStatistics = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }

        metastore.createTable(session, table, principalPrivileges, Optional.of(writeInfo.getWritePath()), false, tableStatistics, emptyList());

        if (handle.getPartitionedBy().isEmpty()) {
            return Optional.of(new HiveOutputMetadata(new HiveOutputInfo(ImmutableList.of(UNPARTITIONED_ID.getPartitionName()), writeInfo.getTargetPath().toString())));
        }

        if (isRespectTableFormat(session)) {
            verify(handle.getPartitionStorageFormat() == handle.getTableStorageFormat());
        }
        for (PartitionUpdate update : partitionUpdates) {
            Map<String, String> partitionParameters = partitionEncryptionParameters;
            if (isPreferManifestsToListFiles(session) && isFileRenamingEnabled(session)) {
                // Store list of file names and sizes in partition metadata when prefer_manifests_to_list_files and file_renaming_enabled are set to true
                partitionParameters = updatePartitionMetadataWithFileNamesAndSizes(update, partitionParameters);
            }
            Partition partition = partitionObjectBuilder.buildPartitionObject(session, table, update, prestoVersion, partitionParameters, Optional.empty(), Optional.empty());
            PartitionStatistics partitionStatistics = createPartitionStatistics(
                    session,
                    update.getStatistics(),
                    columnTypes,
                    getColumnStatistics(partitionComputedStatistics, partition.getValues()), timeZone);
            metastore.addPartition(
                    session,
                    handle.getSchemaName(),
                    handle.getTableName(),
                    table.getStorage().getLocation(),
                    true,
                    partition,
                    update.getWritePath(),
                    partitionStatistics);
        }

        return Optional.of(new HiveOutputMetadata(new HiveOutputInfo(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .collect(toList()), writeInfo.getTargetPath().toString())));
    }

    public static boolean shouldCreateFilesForMissingBuckets(Table table, ConnectorSession session)
    {
        return !table.getTableType().equals(TEMPORARY_TABLE) || shouldCreateEmptyBucketFilesForTemporaryTable(session);
    }

    private MetastoreContext getMetastoreContext(ConnectorSession session)
    {
        return new MetastoreContext(
                session.getIdentity(),
                session.getQueryId(),
                session.getClientInfo(),
                session.getClientTags(),
                session.getSource(),
                getMetastoreHeaders(session),
                isUserDefinedTypeEncodingEnabled(session),
                metastore.getColumnConverterProvider(),
                session.getWarningCollector(),
                session.getRuntimeStats());
    }

    private Column columnHandleToColumn(ConnectorSession session, HiveColumnHandle handle)
    {
        return columnHandleToColumn(getMetastoreContext(session), handle);
    }

    private Properties getSchema(Optional<Partition> partition, Table table)
    {
        return partition.isPresent() ? getHiveSchema(partition.get(), table) : getHiveSchema(table);
    }

    private StorageFormat getStorageFormat(Optional<Partition> partition, Table table)
    {
        return partition.isPresent() ? partition.get().getStorage().getStorageFormat() : table.getStorage().getStorageFormat();
    }

    private List<PartitionUpdate> computePartitionUpdatesForMissingBuckets(
            ConnectorSession session,
            HiveWritableTableHandle handle,
            Table table,
            List<PartitionUpdate> partitionUpdates)
    {
        // avoid creation of PartitionUpdate with empty list of files
        if (!shouldCreateFilesForMissingBuckets(table, session)) {
            return ImmutableList.of();
        }
        HiveStorageFormat storageFormat = table.getPartitionColumns().isEmpty() ? handle.getTableStorageFormat() : handle.getPartitionStorageFormat();

        // empty un-partitioned bucketed table
        if (table.getPartitionColumns().isEmpty() && partitionUpdates.isEmpty()) {
            int bucketCount = handle.getBucketProperty().get().getBucketCount();
            LocationHandle locationHandle = handle.getLocationHandle();
            List<String> fileNamesForMissingBuckets = computeFileNamesForMissingBuckets(
                    session,
                    storageFormat,
                    handle.getCompressionCodec(),
                    bucketCount,
                    ImmutableSet.of());
            return ImmutableList.of(new PartitionUpdate(
                    "",
                    (handle instanceof HiveInsertTableHandle) ? APPEND : NEW,
                    locationHandle.getWritePath(),
                    locationHandle.getTargetPath(),
                    fileNamesForMissingBuckets.stream()
                            .map(fileName -> new FileWriteInfo(fileName, fileName, Optional.of(0L)))
                            .collect(toImmutableList()),
                    0,
                    0,
                    0,
                    isFileRenamingEnabled(session)));
        }

        ImmutableList.Builder<PartitionUpdate> partitionUpdatesForMissingBucketsBuilder = ImmutableList.builder();
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            int bucketCount = handle.getBucketProperty().get().getBucketCount();

            List<String> fileNamesForMissingBuckets = computeFileNamesForMissingBuckets(
                    session,
                    storageFormat,
                    handle.getCompressionCodec(),
                    bucketCount,
                    ImmutableSet.copyOf(getTargetFileNames(partitionUpdate.getFileWriteInfos())));
            partitionUpdatesForMissingBucketsBuilder.add(new PartitionUpdate(
                    partitionUpdate.getName(),
                    partitionUpdate.getUpdateMode(),
                    partitionUpdate.getWritePath(),
                    partitionUpdate.getTargetPath(),
                    fileNamesForMissingBuckets.stream()
                            .map(fileName -> new FileWriteInfo(fileName, fileName, Optional.of(0L)))
                            .collect(toImmutableList()),
                    0,
                    0,
                    0,
                    isFileRenamingEnabled(session)));
        }
        return partitionUpdatesForMissingBucketsBuilder.build();
    }

    private List<String> computeFileNamesForMissingBuckets(
            ConnectorSession session,
            HiveStorageFormat storageFormat,
            HiveCompressionCodec compressionCodec,
            int bucketCount,
            Set<String> existingFileNames)
    {
        if (existingFileNames.size() == bucketCount) {
            // fast path for common case
            return ImmutableList.of();
        }
        String fileExtension = getFileExtension(fromHiveStorageFormat(storageFormat), compressionCodec);
        ImmutableList.Builder<String> missingFileNamesBuilder = ImmutableList.builder();
        for (int i = 0; i < bucketCount; i++) {
            String targetFileName = isFileRenamingEnabled(session) ? String.valueOf(i) : computeBucketedFileName(session.getQueryId(), i) + fileExtension;
            if (!existingFileNames.contains(targetFileName)) {
                missingFileNamesBuilder.add(targetFileName);
            }
        }
        List<String> missingFileNames = missingFileNamesBuilder.build();
        verify(existingFileNames.size() + missingFileNames.size() == bucketCount);
        return missingFileNames;
    }

    @Override
    public HiveInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return beginInsertInternal(session, tableHandle);
    }

    private HiveInsertTableHandle beginInsertInternal(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        verifyJvmTimeZone();

        MetastoreContext metastoreContext = getMetastoreContext(session);

        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();
        Table table = metastore.getTable(metastoreContext, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        tableWritabilityChecker.checkTableWritable(table);

        List<TableConstraint<String>> constraints = metastore.getTableConstraints(metastoreContext, tableName.getSchemaName(), tableName.getTableName());
        if (constraints.stream().anyMatch(constraint -> !(constraint instanceof NotNullConstraint) && constraint.isEnforced())) {
            throw new PrestoException(NOT_SUPPORTED, format("Cannot write to table %s since it has table constraints that are enforced", table.getSchemaTableName().toString()));
        }

        for (Column column : table.getDataColumns()) {
            if (!isWritableType(column.getType())) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("Inserting into Hive table %s with column type %s not supported", tableName, column.getType()));
            }
        }

        List<HiveColumnHandle> handles = hiveColumnHandles(table).stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toList());

        HiveStorageFormat tableStorageFormat = extractHiveStorageFormat(table);
        LocationHandle locationHandle;
        boolean isTemporaryTable = table.getTableType().equals(TEMPORARY_TABLE);
        boolean tempPathRequired = isTempPathRequired(
                session,
                table.getStorage().getBucketProperty(),
                decodePreferredOrderingColumnsFromStorage(table.getStorage()));
        if (isTemporaryTable) {
            locationHandle = locationService.forTemporaryTable(metastore, session, table, tempPathRequired);
        }
        else {
            locationHandle = locationService.forExistingTable(metastore, session, table, tempPathRequired);
        }

        Optional<? extends TableEncryptionProperties> tableEncryptionProperties = getTableEncryptionPropertiesFromHiveProperties(table.getParameters(), tableStorageFormat);

        HiveStorageFormat partitionStorageFormat = isRespectTableFormat(session) ? tableStorageFormat : getHiveStorageFormat(session);
        HiveStorageFormat actualStorageFormat = table.getPartitionColumns().isEmpty() ? tableStorageFormat : partitionStorageFormat;

        if (tableEncryptionProperties.isPresent() && actualStorageFormat != tableStorageFormat) {
            throw new PrestoException(
                    INVALID_TABLE_PROPERTY,
                    format(
                            "For encrypted tables, partition format (%s) should match table format (%s). Using the session property %s or appropriately setting %s can help with ensuring this",
                            actualStorageFormat.name(),
                            tableStorageFormat.name(),
                            RESPECT_TABLE_FORMAT,
                            HIVE_STORAGE_FORMAT));
        }

        HiveInsertTableHandle result = new HiveInsertTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                handles,
                metastore.generatePageSinkMetadata(metastoreContext, tableName),
                locationHandle,
                table.getStorage().getBucketProperty(),
                decodePreferredOrderingColumnsFromStorage(table.getStorage()),
                tableStorageFormat,
                partitionStorageFormat,
                actualStorageFormat,
                getHiveCompressionCodec(session, isTemporaryTable, actualStorageFormat),
                encryptionInformationProvider.getWriteEncryptionInformation(session, tableEncryptionProperties.map(identity()), tableName.getSchemaName(), tableName.getTableName()));

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);

        if (getInsertExistingPartitionsBehavior(session) == InsertExistingPartitionsBehavior.OVERWRITE
                && isTransactionalTable(table.getParameters())
                && writeInfo.getWriteMode() == DIRECT_TO_TARGET_EXISTING_DIRECTORY) {
            throw new PrestoException(NOT_SUPPORTED, "Overwriting existing partition in transactional tables doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode");
        }

        metastore.declareIntentionToWrite(
                new HdfsContext(session, tableName.getSchemaName(), tableName.getTableName(), table.getStorage().getLocation(), false),
                metastoreContext,
                writeInfo.getWriteMode(),
                writeInfo.getWritePath(),
                writeInfo.getTempPath(),
                tableName,
                isTemporaryTable);
        return result;
    }

    private HiveCompressionCodec getHiveCompressionCodec(ConnectorSession session, boolean isTemporaryTable, HiveStorageFormat storageFormat)
    {
        if (isTemporaryTable) {
            return getTemporaryTableCompressionCodec(session);
        }
        if (storageFormat == ORC || storageFormat == DWRF) {
            return getOrcCompressionCodec(session);
        }
        return getCompressionCodec(session);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsertInternal(session, insertHandle, fragments, computedStatistics);
    }

    private Optional<ConnectorOutputMetadata> finishInsertInternal(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) insertHandle;

        List<PartitionUpdate> partitionUpdates = getPartitionUpdates(session, fragments);

        HiveStorageFormat tableStorageFormat = handle.getTableStorageFormat();
        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        MetastoreContext metastoreContext = getMetastoreContext(session);

        Table table = metastore.getTable(metastoreContext, handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (!table.getStorage().getStorageFormat().getInputFormat().equals(tableStorageFormat.getInputFormat()) && isRespectTableFormat(session)) {
            throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during insert");
        }

        if (handle.getBucketProperty().isPresent() && isCreateEmptyBucketFiles(session)) {
            List<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(
                    session,
                    handle,
                    table,
                    partitionUpdates);
            // replace partitionUpdates before creating the zero-row files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            HdfsContext hdfsContext = new HdfsContext(session, table.getDatabaseName(), table.getTableName(), table.getStorage().getLocation(), false);
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() :
                        Optional.of(partitionObjectBuilder.buildPartitionObject(
                                session,
                                table,
                                partitionUpdate,
                                prestoVersion,
                                handle.getEncryptionInformation()
                                        .map(encryptionInfo -> encryptionInfo.getDwrfEncryptionMetadata().map(DwrfEncryptionMetadata::getExtraMetadata).orElseGet(ImmutableMap::of))
                                        .orElseGet(ImmutableMap::of),
                                Optional.empty(),
                                Optional.empty()));
                zeroRowFileCreator.createFiles(
                        session,
                        hdfsContext,
                        partitionUpdate.getWritePath(),
                        getTargetFileNames(partitionUpdate.getFileWriteInfos()),
                        getStorageFormat(partition, table),
                        handle.getCompressionCodec(),
                        getSchema(partition, table));
            }
        }

        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = createComputedStatisticsToPartitionMap(computedStatistics, partitionedBy, columnTypes);

        Map<String, Optional<Partition>> existingPartitions = getExistingPartitionsByNames(metastoreContext, handle.getSchemaName(), handle.getTableName(), partitionUpdates);

        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            if (partitionUpdate.getName().isEmpty()) {
                if (handle.getEncryptionInformation().isPresent()) {
                    throw new PrestoException(HIVE_UNSUPPORTED_ENCRYPTION_OPERATION, "Inserting into an existing table with encryption enabled is not supported yet");
                }

                // insert into unpartitioned table
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        session,
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, ImmutableList.of()), timeZone);
                metastore.finishInsertIntoExistingTable(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionUpdate.getWritePath(),
                        getTargetFileNames(partitionUpdate.getFileWriteInfos()),
                        partitionStatistics);
            }
            else if (partitionUpdate.getUpdateMode() == APPEND) {
                if (handle.getEncryptionInformation().isPresent()) {
                    throw new PrestoException(HIVE_UNSUPPORTED_ENCRYPTION_OPERATION, "Inserting into an existing partition with encryption enabled is not supported yet");
                }
                // insert into existing partition
                String partitionName = partitionUpdate.getName();
                List<String> partitionValues = toPartitionValues(partitionName);
                List<Type> partitionTypes = partitionedBy.stream()
                        .map(columnTypes::get)
                        .collect(toImmutableList());
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        session,
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partitionName, partitionValues, partitionTypes), timeZone);
                metastore.finishInsertIntoExistingPartition(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        handle.getLocationHandle().getTargetPath().toString(),
                        partitionValues,
                        partitionUpdate.getWritePath(),
                        getTargetFileNames(partitionUpdate.getFileWriteInfos()),
                        partitionStatistics);
            }
            else if (partitionUpdate.getUpdateMode() == NEW || partitionUpdate.getUpdateMode() == OVERWRITE) {
                Map<String, String> extraPartitionMetadata = handle.getEncryptionInformation()
                        .map(encryptionInfo -> encryptionInfo.getDwrfEncryptionMetadata().map(DwrfEncryptionMetadata::getExtraMetadata).orElseGet(ImmutableMap::of))
                        .orElseGet(ImmutableMap::of);

                if (isPreferManifestsToListFiles(session) && isFileRenamingEnabled(session)) {
                    // Store list of file names and sizes in partition metadata when prefer_manifests_to_list_files and file_renaming_enabled are set to true
                    extraPartitionMetadata = updatePartitionMetadataWithFileNamesAndSizes(partitionUpdate, extraPartitionMetadata);
                }

                // Track the manifest blob size
                getManifestSizeInBytes(session, partitionUpdate, extraPartitionMetadata).ifPresent(hivePartitionStats::addManifestSizeInBytes);

                boolean isExistingPartition = existingPartitions.containsKey(partitionUpdate.getName());
                Optional<Partition> existingPartition = Optional.empty();
                if (isExistingPartition) {
                    // Overwriting an existing partition
                    if (partitionUpdate.getUpdateMode() == OVERWRITE) {
                        existingPartition = existingPartitions.get(partitionUpdate.getName());
                        if (handle.getLocationHandle().getWriteMode() == DIRECT_TO_TARGET_EXISTING_DIRECTORY) {
                            // In this writeMode, the new files will be written to the same directory. Since this is
                            // an overwrite operation, we must remove all the old files not written by current query.
                            removeNonCurrentQueryFiles(session, partitionUpdate.getTargetPath());
                        }
                        else {
                            metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), handle.getLocationHandle().getTargetPath().toString(), extractPartitionValues(partitionUpdate.getName()));
                        }
                    }
                    else {
                        throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Cannot insert into an existing partition of Hive table: " + partitionUpdate.getName());
                    }
                }
                // insert into new partition or overwrite existing partition
                Partition partition = partitionObjectBuilder.buildPartitionObject(
                        session,
                        table,
                        partitionUpdate,
                        prestoVersion,
                        extraPartitionMetadata,
                        existingPartition,
                        Optional.empty());
                if (!partition.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && isRespectTableFormat(session)) {
                    throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Partition format changed during insert");
                }

                String partitionName = partitionUpdate.getName();
                List<String> partitionValues = partition.getValues();
                List<Type> partitionTypes = partitionedBy.stream()
                        .map(columnTypes::get)
                        .collect(toImmutableList());
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        session,
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partitionName, partitionValues, partitionTypes),
                        timeZone);

                // New partition or overwriting existing partition by staging and moving the new partition
                if (!isExistingPartition || handle.getLocationHandle().getWriteMode() != DIRECT_TO_TARGET_EXISTING_DIRECTORY) {
                    metastore.addPartition(
                            session,
                            handle.getSchemaName(),
                            handle.getTableName(),
                            table.getStorage().getLocation(),
                            false,
                            partition,
                            partitionUpdate.getWritePath(),
                            partitionStatistics);
                }
            }
            else {
                throw new IllegalArgumentException(format("Unsupported update mode: %s", partitionUpdate.getUpdateMode()));
            }
        }

        return Optional.of(new HiveOutputMetadata(new HiveOutputInfo(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .map(name -> name.isEmpty() ? UNPARTITIONED_ID.getPartitionName() : name)
                        .collect(toList()), table.getStorage().getLocation())));
    }

    /**
     * Deletes all the files not written by the current query from the given partition path.
     * This is required when we are overwriting the partitions by directly writing the new
     * files to the existing directory, where files written by older queries may be present too.
     *
     * @param session the ConnectorSession object
     * @param partitionPath the path of the partition from where the older files are to be deleted
     */
    private void removeNonCurrentQueryFiles(ConnectorSession session, Path partitionPath)
    {
        String queryId = session.getQueryId();
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsContext(session), partitionPath);
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(partitionPath, false);
            while (iterator.hasNext()) {
                Path file = iterator.next().getPath();
                if (!isFileCreatedByQuery(file.getName(), queryId)) {
                    fileSystem.delete(file, false);
                }
            }
        }
        catch (Exception ex) {
            throw new PrestoException(
                    HIVE_FILESYSTEM_ERROR,
                    format("Failed to delete partition %s files during overwrite", partitionPath),
                    ex);
        }
    }

    private static boolean isTempPathRequired(ConnectorSession session, Optional<HiveBucketProperty> bucketProperty, List<SortingColumn> preferredOrderingColumns)
    {
        boolean hasSortedWrite = bucketProperty.map(property -> !property.getSortedBy().isEmpty()).orElse(false) || !preferredOrderingColumns.isEmpty();
        return isSortedWriteToTempPathEnabled(session) && hasSortedWrite;
    }

    private List<String> getTargetFileNames(List<FileWriteInfo> fileWriteInfos)
    {
        return fileWriteInfos.stream()
                .map(FileWriteInfo::getTargetFileName)
                .collect(toImmutableList());
    }

    private Map<String, Optional<Partition>> getExistingPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionUpdate> partitionUpdates)
    {
        ImmutableMap.Builder<String, Optional<Partition>> existingPartitions = ImmutableMap.builder();
        ImmutableSet.Builder<String> potentiallyNewPartitions = ImmutableSet.builder();

        for (PartitionUpdate update : partitionUpdates) {
            switch (update.getUpdateMode()) {
                case APPEND:
                    existingPartitions.put(update.getName(), Optional.empty());
                    break;
                case NEW:
                case OVERWRITE:
                    potentiallyNewPartitions.add(update.getName());
                    break;
                default:
                    throw new IllegalArgumentException("unexpected update mode: " + update.getUpdateMode());
            }
        }

        // try to load potentially new partitions in batches to check if any of them exist
        Lists.partition(ImmutableList.copyOf(potentiallyNewPartitions.build()), maxPartitionBatchSize).stream()
                .flatMap(partitionNames -> metastore.getPartitionsByNames(metastoreContext, databaseName, tableName, getPartitionNamesWithEmptyVersion(partitionNames)).entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent()))
                .forEach(entry -> existingPartitions.put(entry.getKey(), entry.getValue()));

        return existingPartitions.build();
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        SchemaTableName viewName = viewMetadata.getTable();
        Table table = createTableObjectForViewCreation(
                session,
                viewMetadata,
                createViewProperties(session, prestoVersion),
                typeTranslator,
                metastoreContext,
                encodeViewData(viewData));
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(session.getUser());

        Optional<Table> existing = metastore.getTable(metastoreContext, viewName.getSchemaName(), viewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !MetastoreUtil.isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(viewName);
            }

            metastore.replaceView(metastoreContext, viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(session, table, principalPrivileges, Optional.empty(), false, new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()), emptyList());
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorViewDefinition view = getViews(session, viewName.toSchemaTablePrefix()).get(viewName);
        checkIfNullView(view, viewName);

        try {
            metastore.dropTable(
                    new HdfsContext(session, viewName.getSchemaName()),
                    viewName.getSchemaName(),
                    viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        MetastoreContext metastoreContext = getMetastoreContext(session);
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : metastore.getAllViews(metastoreContext, schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, prefix.getSchemaName());
        }

        MetastoreContext metastoreContext = getMetastoreContext(session);
        for (SchemaTableName schemaTableName : tableNames) {
            Optional<Table> table = metastore.getTable(metastoreContext, schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (table.isPresent() && isPrestoView(table.get())) {
                verifyAndPopulateViews(table.get(), schemaTableName, decodeViewData(table.get().getViewOriginalText().get()), views);
            }
        }
        return views.build();
    }

    @Override
    public Optional<MaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        requireNonNull(viewName, "viewName is null");

        MetastoreContext metastoreContext = getMetastoreContext(session);
        Optional<Table> table = metastore.getTable(metastoreContext, viewName.getSchemaName(), viewName.getTableName());

        if (table.isPresent() && MetastoreUtil.isPrestoMaterializedView(table.get())) {
            try {
                return Optional.of(MATERIALIZED_VIEW_JSON_CODEC.fromJson(decodeMaterializedViewData(table.get().getViewOriginalText().get())));
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_VIEW, "Invalid materialized view JSON", e);
            }
        }

        return Optional.empty();
    }

    @Override
    public MaterializedViewStatus getMaterializedViewStatus(ConnectorSession session, SchemaTableName materializedViewName, TupleDomain<String> baseQueryDomain)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        MaterializedViewDefinition viewDefinition = getMaterializedView(session, materializedViewName)
                .orElseThrow(() -> new MaterializedViewNotFoundException(materializedViewName));

        List<Table> baseTables = viewDefinition.getBaseTables().stream()
                .map(baseTableName -> metastore.getTable(metastoreContext, baseTableName.getSchemaName(), baseTableName.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(baseTableName)))
                .collect(toImmutableList());

        baseTables.forEach(table -> checkState(
                table.getTableType().equals(MANAGED_TABLE),
                format("base table %s is not a managed table", table.getTableName())));

        Table materializedViewTable = metastore.getTable(metastoreContext, materializedViewName.getSchemaName(), materializedViewName.getTableName())
                .orElseThrow(() -> new MaterializedViewNotFoundException(materializedViewName));

        checkState(
                materializedViewTable.getTableType().equals(MATERIALIZED_VIEW),
                format("materialized view table %s is not a materialized view", materializedViewTable.getTableName()));

        validateMaterializedViewPartitionColumns(metastore, metastoreContext, materializedViewTable, viewDefinition);
        Map<String, Map<SchemaTableName, String>> directColumnMappings = viewDefinition.getDirectColumnMappingsAsMap();
        Map<SchemaTableName, Map<String, String>> viewToBasePartitionMap = getViewToBasePartitionMap(materializedViewTable, baseTables, directColumnMappings);

        MaterializedDataPredicates materializedDataPredicates = getMaterializedDataPredicates(metastore, metastoreContext, typeManager, materializedViewTable, timeZone);
        if (materializedDataPredicates.getPredicateDisjuncts().isEmpty()) {
            return new MaterializedViewStatus(NOT_MATERIALIZED);
        }

        // Partitions to keep track of for materialized view freshness are the partitions of every base table
        // that are not available/updated to the materialized view yet.
        Map<SchemaTableName, MaterializedDataPredicates> partitionsFromBaseTables = baseTables.stream()
                .collect(toImmutableMap(
                        baseTable -> new SchemaTableName(baseTable.getDatabaseName(), baseTable.getTableName()),
                        baseTable -> {
                            MaterializedDataPredicates baseTableMaterializedPredicates = getMaterializedDataPredicates(metastore, metastoreContext, typeManager, baseTable, timeZone);
                            SchemaTableName schemaTableName = new SchemaTableName(baseTable.getDatabaseName(), baseTable.getTableName());
                            Map<String, String> viewToBaseIndirectMappedColumns = viewToBaseTableOnOuterJoinSideIndirectMappedPartitions(viewDefinition, baseTable).orElse(ImmutableMap.of());

                            return differenceDataPredicates(
                                    baseTableMaterializedPredicates,
                                    materializedDataPredicates,
                                    viewToBasePartitionMap.getOrDefault(schemaTableName, ImmutableMap.of()),
                                    viewToBaseIndirectMappedColumns);
                        }));

        int missingPartitions = 0;

        for (MaterializedDataPredicates dataPredicates : partitionsFromBaseTables.values()) {
            if (!dataPredicates.getPredicateDisjuncts().isEmpty()) {
                missingPartitions += dataPredicates.getPredicateDisjuncts().stream()
                        .filter(baseQueryDomain::overlaps)
                        .mapToInt(tupleDomain -> tupleDomain.getDomains().isPresent() ? tupleDomain.getDomains().get().size() : 0)
                        .sum();
            }
        }

        if (missingPartitions > HiveSessionProperties.getMaterializedViewMissingPartitionsThreshold(session)) {
            return new MaterializedViewStatus(TOO_MANY_PARTITIONS_MISSING, partitionsFromBaseTables);
        }
        if (missingPartitions != 0) {
            return new MaterializedViewStatus(PARTIALLY_MATERIALIZED, partitionsFromBaseTables);
        }

        return new MaterializedViewStatus(FULLY_MATERIALIZED);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, ConnectorTableMetadata viewMetadata, MaterializedViewDefinition viewDefinition, boolean ignoreExisting)
    {
        if (isExternalTable(viewMetadata.getProperties())) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Specifying external location for materialized view is not supported.");
        }

        Table basicTable = prepareTable(session, viewMetadata, MATERIALIZED_VIEW);
        viewDefinition = new MaterializedViewDefinition(
                viewDefinition.getOriginalSql(),
                viewDefinition.getSchema(),
                viewDefinition.getTable(),
                viewDefinition.getBaseTables(),
                viewDefinition.getOwner(),
                viewDefinition.getColumnMappings(),
                viewDefinition.getBaseTablesOnOuterJoinSide(),
                Optional.of(getPartitionedBy(viewMetadata.getProperties())));
        Map<String, String> parameters = ImmutableMap.<String, String>builder()
                .putAll(basicTable.getParameters())
                .put(PRESTO_MATERIALIZED_VIEW_FLAG, "true")
                .build();
        Table viewTable = Table.builder(basicTable)
                .setParameters(parameters)
                .setViewOriginalText(Optional.of(encodeMaterializedViewData(MATERIALIZED_VIEW_JSON_CODEC.toJson(viewDefinition))))
                .setViewExpandedText(Optional.of("/* Presto Materialized View */"))
                .build();

        MetastoreContext metastoreContext = getMetastoreContext(session);
        validateMaterializedViewPartitionColumns(metastore, metastoreContext, viewTable, viewDefinition);

        try {
            PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(viewTable.getOwner());
            metastore.createTable(
                    session,
                    viewTable,
                    principalPrivileges,
                    Optional.empty(),
                    ignoreExisting,
                    new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()),
                    emptyList());
        }
        catch (TableAlreadyExistsException e) {
            throw new MaterializedViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<MaterializedViewDefinition> view = getMaterializedView(session, viewName);
        if (!view.isPresent()) {
            throw new MaterializedViewNotFoundException(viewName);
        }

        try {
            metastore.dropTable(
                    new HdfsContext(session, viewName.getSchemaName(), viewName.getTableName()),
                    viewName.getSchemaName(),
                    viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new MaterializedViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public HiveInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return beginInsertInternal(session, tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsertInternal(session, insertHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<List<SchemaTableName>> getReferencedMaterializedViews(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Table table = metastore.getTable(metastoreContext, tableName.getSchemaName(), tableName.getTableName()).orElseThrow(() -> new TableNotFoundException(tableName));
        if (!table.getTableType().equals(MANAGED_TABLE) || MetastoreUtil.isPrestoMaterializedView(table)) {
            return Optional.empty();
        }
        ImmutableList.Builder<SchemaTableName> materializedViews = ImmutableList.builder();
        for (String viewName : Splitter.on(",").trimResults().omitEmptyStrings().splitToList(table.getParameters().getOrDefault(REFERENCED_MATERIALIZED_VIEWS, ""))) {
            materializedViews.add(SchemaTableName.valueOf(viewName));
        }
        return Optional.of(materializedViews.build());
    }

    @Override
    public ConnectorDeleteTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely");
    }

    @Override
    public Optional<ColumnHandle> getDeleteRowIdColumn(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Optional.of(updateRowIdHandle());
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableLayoutHandle;

        MetastoreContext metastoreContext = getMetastoreContext(session);
        Optional<Table> table = metastore.getTable(metastoreContext, handle);
        if (!table.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        if (table.get().getPartitionColumns().isEmpty()) {
            metastore.truncateUnpartitionedTable(session, handle.getSchemaName(), handle.getTableName());
        }
        else {
            for (HivePartition hivePartition : getOrComputePartitions(layoutHandle, session, tableHandle)) {
                metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), table.get().getStorage().getLocation(), toPartitionValues(hivePartition.getPartitionId().getPartitionName()));
            }
        }
        // it is too expensive to determine the exact number of deleted rows
        return OptionalLong.empty();
    }

    private List<HivePartition> getOrComputePartitions(HiveTableLayoutHandle layoutHandle, ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (layoutHandle.getPartitions().isPresent()) {
            return layoutHandle.getPartitions().get();
        }
        else {
            TupleDomain<ColumnHandle> partitionColumnPredicate = layoutHandle.getPartitionColumnPredicate();
            Predicate<Map<ColumnHandle, NullableValue>> predicate = convertToPredicate(partitionColumnPredicate);
            ConnectorTableLayoutResult tableLayoutResult = getTableLayoutForConstraint(session, tableHandle, new Constraint<>(partitionColumnPredicate, predicate), Optional.empty());
            return ((HiveTableLayoutHandle) tableLayoutResult.getTableLayout().getHandle()).getPartitions().get();
        }
    }

    @VisibleForTesting
    static Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> tupleDomain)
    {
        return bindings -> tupleDomain.contains(TupleDomain.fromFixedValues(bindings));
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        if (!tableLayoutHandle.isPresent()) {
            return true;
        }

        // Allow metadata delete for range filters on partition columns.
        // TODO Add support for metadata delete for any filter on partition columns.

        HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableLayoutHandle.get();
        if (!layoutHandle.isPushdownFilterEnabled()) {
            return true;
        }

        if (layoutHandle.getPredicateColumns().isEmpty()) {
            return true;
        }

        if (!TRUE_CONSTANT.equals(layoutHandle.getRemainingPredicate())) {
            return false;
        }

        TupleDomain<Subfield> domainPredicate = layoutHandle.getDomainPredicate();
        if (domainPredicate.isAll()) {
            return true;
        }

        Set<String> predicateColumnNames = domainPredicate.getDomains().get().keySet().stream()
                .map(Subfield::getRootName)
                .collect(toImmutableSet());

        Set<String> partitionColumnNames = layoutHandle.getPartitionColumns().stream()
                .map(BaseHiveColumnHandle::getName)
                .collect(toImmutableSet());

        return partitionColumnNames.containsAll(predicateColumnNames);
    }

    @Override
    public boolean isLegacyGetLayoutSupported(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (((HiveTableHandle) tableHandle).getAnalyzePartitionValues().isPresent()) {
            return true;
        }

        return !isPushdownFilterEnabled(session, tableHandle);
    }

    private String createTableLayoutString(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter,
            RowExpression remainingPredicate,
            TupleDomain<Subfield> domainPredicate)
    {
        return toStringHelper(tableName.toString())
                .omitNullValues()
                .add("buckets", bucketHandle.map(HiveBucketHandle::getReadBucketCount).orElse(null))
                .add("bucketsToKeep", bucketFilter.map(HiveBucketFilter::getBucketsToKeep).orElse(null))
                .add("filter", TRUE_CONSTANT.equals(remainingPredicate) ? null : rowExpressionService.formatRowExpression(session, remainingPredicate))
                .add("domains", domainPredicate.isAll() ? null : domainPredicate.toString(session.getSqlFunctionProperties()))
                .toString();
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        HivePartitionResult hivePartitionResult;
        if (handle.getAnalyzePartitionValues().isPresent()) {
            verify(constraint.getSummary().isAll(), "There shouldn't be any constraint for ANALYZE operation");
            hivePartitionResult = partitionManager.getPartitions(metastore, tableHandle, handle.getAnalyzePartitionValues().get(), session);
        }
        else {
            hivePartitionResult = partitionManager.getPartitions(metastore, tableHandle, constraint, session);
        }

        Map<String, HiveColumnHandle> predicateColumns = hivePartitionResult.getEffectivePredicate().getDomains().get().keySet().stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toImmutableMap(HiveColumnHandle::getName, Functions.identity()));

        Optional<HiveBucketHandle> hiveBucketHandle = hivePartitionResult.getBucketHandle();
        int virtualBucketCount = getVirtualBucketCount(session);
        if (!hivePartitionResult.getBucketHandle().isPresent() && virtualBucketCount > 0) {
            hiveBucketHandle = Optional.of(createVirtualBucketHandle(virtualBucketCount));
        }

        TupleDomain<Subfield> domainPredicate = hivePartitionResult.getEffectivePredicate().transform(HiveMetadata::toSubfield);
        Table table = metastore.getTable(getMetastoreContext(session), handle)
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));

        String layoutString = createTableLayoutString(session, handle.getSchemaTableName(), hivePartitionResult.getBucketHandle(), hivePartitionResult.getBucketFilter(), TRUE_CONSTANT, domainPredicate);
        Optional<Set<HiveColumnHandle>> requestedColumns = desiredColumns.map(columns -> columns.stream().map(column -> (HiveColumnHandle) column).collect(toImmutableSet()));
        return new ConnectorTableLayoutResult(
                getTableLayout(
                        session,
                        new HiveTableLayoutHandle.Builder()
                                .setSchemaTableName(handle.getSchemaTableName())
                                .setTablePath(table.getStorage().getLocation())
                                .setPartitionColumns(hivePartitionResult.getPartitionColumns())
                                .setDataColumns(pruneColumnComments(hivePartitionResult.getDataColumns()))
                                .setTableParameters(hivePartitionResult.getTableParameters())
                                .setDomainPredicate(domainPredicate)
                                .setRemainingPredicate(TRUE_CONSTANT)
                                .setPredicateColumns(predicateColumns)
                                .setPartitionColumnPredicate(hivePartitionResult.getEnforcedConstraint())
                                .setPartitions(hivePartitionResult.getPartitions())
                                .setBucketHandle(hiveBucketHandle)
                                .setBucketFilter(hivePartitionResult.getBucketFilter())
                                .setPushdownFilterEnabled(false)
                                .setLayoutString(layoutString)
                                .setRequestedColumns(requestedColumns)
                                .setPartialAggregationsPushedDown(false)
                                .setAppendRowNumberEnabled(false)
                                .setHiveTableHandle(handle)
                                .build()),
                hivePartitionResult.getUnenforcedConstraint());
    }

    private static Subfield toSubfield(ColumnHandle columnHandle)
    {
        return new Subfield(((HiveColumnHandle) columnHandle).getName(), ImmutableList.of());
    }

    private boolean isPushdownFilterEnabled(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        boolean pushdownFilterEnabled = HiveSessionProperties.isPushdownFilterEnabled(session);
        if (pushdownFilterEnabled) {
            HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(getTableMetadata(session, tableHandle).getProperties());
            if (hiveStorageFormat == ORC || hiveStorageFormat == DWRF || hiveStorageFormat == PARQUET && isParquetPushdownFilterEnabled(session)) {
                return true;
            }
        }
        return false;
    }

    private List<Column> pruneColumnComments(List<Column> columns)
    {
        return columns.stream()
                .map(column -> new Column(column.getName(), column.getType(), Optional.empty(), column.getTypeMetadata()))
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        HiveTableLayoutHandle hiveLayoutHandle = (HiveTableLayoutHandle) layoutHandle;
        List<ColumnHandle> partitionColumns = ImmutableList.copyOf(hiveLayoutHandle.getPartitionColumns());
        List<HivePartition> partitions = hiveLayoutHandle.getPartitions().get();

        Optional<DiscretePredicates> discretePredicates = getDiscretePredicates(partitionColumns, partitions);

        Optional<ConnectorTablePartitioning> tablePartitioning = Optional.empty();
        SchemaTableName tableName = hiveLayoutHandle.getSchemaTableName();
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Table table = hiveLayoutHandle.getTable(metastore, metastoreContext);
        // never ignore table bucketing for temporary tables as those are created such explicitly by the engine request
        boolean bucketExecutionEnabled = table.getTableType().equals(TEMPORARY_TABLE) || isBucketExecutionEnabled(session);
        if (bucketExecutionEnabled && hiveLayoutHandle.getBucketHandle().isPresent()) {
            HiveBucketHandle hiveBucketHandle = hiveLayoutHandle.getBucketHandle().get();
            HivePartitioningHandle partitioningHandle;
            int bucketCount = hiveBucketHandle.getReadBucketCount();
            OptionalInt maxCompatibleBucketCount = OptionalInt.empty();

            // Virtually bucketed table does not have table bucket property
            if (hiveBucketHandle.isVirtuallyBucketed()) {
                partitioningHandle = createHiveCompatiblePartitioningHandle(
                        bucketCount,
                        hiveBucketHandle.getColumns().stream()
                                .map(HiveColumnHandle::getHiveType)
                                .collect(toImmutableList()),
                        maxCompatibleBucketCount);
            }
            else {
                HiveBucketProperty bucketProperty = table.getStorage().getBucketProperty()
                        .orElseThrow(() -> new IllegalArgumentException("bucketProperty is expected to be present"));
                switch (bucketProperty.getBucketFunctionType()) {
                    case HIVE_COMPATIBLE:
                        partitioningHandle = createHiveCompatiblePartitioningHandle(
                                bucketCount,
                                hiveBucketHandle.getColumns().stream()
                                        .map(HiveColumnHandle::getHiveType)
                                        .collect(toImmutableList()),
                                maxCompatibleBucketCount);
                        break;
                    case PRESTO_NATIVE:
                        partitioningHandle = createPrestoNativePartitioningHandle(
                                bucketCount,
                                bucketProperty.getTypes().get(),
                                maxCompatibleBucketCount);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported bucket function type " + bucketProperty.getBucketFunctionType());
                }
            }

            tablePartitioning = Optional.of(new ConnectorTablePartitioning(
                    partitioningHandle,
                    hiveBucketHandle.getColumns().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList())));
        }

        TupleDomain<ColumnHandle> predicate;
        RowExpression subfieldPredicate;
        if (hiveLayoutHandle.isPushdownFilterEnabled()) {
            Map<String, ColumnHandle> predicateColumns = hiveLayoutHandle.getPredicateColumns().entrySet()
                    .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            predicate = getPredicate(hiveLayoutHandle, partitionColumns, partitions, predicateColumns);

            // capture subfields from domainPredicate to add to remainingPredicate
            // so those filters don't get lost
            Map<String, Type> columnTypes = hiveColumnHandles(table).stream()
                    .collect(toImmutableMap(HiveColumnHandle::getName, columnHandle -> columnHandle.getColumnMetadata(typeManager,
                            normalizeIdentifier(session, columnHandle.getName())).getType()));

            subfieldPredicate = getSubfieldPredicate(session, hiveLayoutHandle, columnTypes, functionResolution, rowExpressionService);
        }
        else {
            predicate = createPredicate(partitionColumns, partitions);
            subfieldPredicate = TRUE_CONSTANT;
        }

        // Expose ordering property of the table when order based execution is enabled.
        ImmutableList.Builder<LocalProperty<ColumnHandle>> localPropertyBuilder = ImmutableList.builder();
        Optional<Set<ColumnHandle>> streamPartitionColumns = Optional.empty();
        if (table.getStorage().getBucketProperty().isPresent()
                && !table.getStorage().getBucketProperty().get().getSortedBy().isEmpty()
                && isOrderBasedExecutionEnabled(session)) {
            ImmutableSet.Builder<ColumnHandle> streamPartitionColumnsBuilder = ImmutableSet.builder();
            Map<String, ColumnHandle> columnHandles = hiveColumnHandles(table).stream()
                    .collect(toImmutableMap(HiveColumnHandle::getName, identity()));

            // streamPartitioningColumns is how we partition the data across splits.
            // localProperty is how we partition the data within a split.

            // 1. add partition columns and bucketed-by columns to streamPartitionColumns
            // when order based execution is enabled, splitting is disabled and data is sharded across splits when table is bucketed.
            partitionColumns.forEach(streamPartitionColumnsBuilder::add);
            table.getStorage().getBucketProperty().get().getBucketedBy().forEach(bucketedByColumn -> {
                ColumnHandle columnHandle = columnHandles.get(bucketedByColumn);
                streamPartitionColumnsBuilder.add(columnHandle);
            });

            // 2. add sorted-by columns to localPropertyBuilder
            table.getStorage().getBucketProperty().get().getSortedBy().forEach(sortingColumn -> {
                ColumnHandle columnHandle = columnHandles.get(sortingColumn.getColumnName());
                localPropertyBuilder.add(new SortingProperty<>(columnHandle, sortingColumn.getOrder().getSortOrder()));
            });
            streamPartitionColumns = Optional.of(streamPartitionColumnsBuilder.build());
        }

        // combine subfieldPredicate with remainingPredicate
        RowExpression combinedRemainingPredicate = getCombinedRemainingPredicate(hiveLayoutHandle, subfieldPredicate);

        return new ConnectorTableLayout(
                hiveLayoutHandle,
                Optional.empty(),
                predicate,
                tablePartitioning,
                streamPartitionColumns,
                discretePredicates,
                localPropertyBuilder.build(),
                Optional.of(combinedRemainingPredicate),
                Optional.of(rowIdColumnHandle()));
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        HivePartitioningHandle leftHandle = (HivePartitioningHandle) left;
        HivePartitioningHandle rightHandle = (HivePartitioningHandle) right;

        if (!leftHandle.getBucketFunctionType().equals(rightHandle.getBucketFunctionType()) ||
                !leftHandle.getHiveTypes().equals(rightHandle.getHiveTypes()) ||
                !leftHandle.getTypes().equals(rightHandle.getTypes())) {
            return Optional.empty();
        }
        if (leftHandle.getBucketCount() == rightHandle.getBucketCount()) {
            return Optional.of(leftHandle);
        }
        if (!isOptimizedMismatchedBucketCount(session)) {
            return Optional.empty();
        }

        int largerBucketCount = Math.max(leftHandle.getBucketCount(), rightHandle.getBucketCount());
        int smallerBucketCount = Math.min(leftHandle.getBucketCount(), rightHandle.getBucketCount());
        if (largerBucketCount % smallerBucketCount != 0) {
            // must be evenly divisible
            return Optional.empty();
        }
        if (Integer.bitCount(largerBucketCount / smallerBucketCount) != 1) {
            // ratio must be power of two
            return Optional.empty();
        }

        OptionalInt maxCompatibleBucketCount = min(leftHandle.getMaxCompatibleBucketCount(), rightHandle.getMaxCompatibleBucketCount());
        if (maxCompatibleBucketCount.isPresent() && maxCompatibleBucketCount.getAsInt() < smallerBucketCount) {
            // maxCompatibleBucketCount must be larger than or equal to smallerBucketCount
            // because the current code uses the smallerBucketCount as the common partitioning handle.
            return Optional.empty();
        }

        return Optional.of(new HivePartitioningHandle(
                smallerBucketCount,
                maxCompatibleBucketCount,
                leftHandle.getBucketFunctionType(),
                leftHandle.getHiveTypes(),
                leftHandle.getTypes()));
    }

    @Override
    public boolean isRefinedPartitioningOver(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        HivePartitioningHandle leftHandle = (HivePartitioningHandle) left;
        HivePartitioningHandle rightHandle = (HivePartitioningHandle) right;
        if (!leftHandle.getBucketFunctionType().equals(rightHandle.getBucketFunctionType()) ||
                !leftHandle.getHiveTypes().equals(rightHandle.getHiveTypes()) ||
                !leftHandle.getTypes().equals(rightHandle.getTypes())) {
            return false;
        }

        int leftBucketCount = leftHandle.getBucketCount();
        int rightBucketCount = rightHandle.getBucketCount();
        return leftBucketCount == rightBucketCount || // must be evenly divisible
                (leftBucketCount % rightBucketCount == 0 &&
                        // ratio must be power of two
                        // TODO: this can be relaxed
                        Integer.bitCount(leftBucketCount / rightBucketCount) == 1);
    }

    private static OptionalInt min(OptionalInt left, OptionalInt right)
    {
        if (!left.isPresent()) {
            return right;
        }
        if (!right.isPresent()) {
            return left;
        }
        return OptionalInt.of(Math.min(left.getAsInt(), right.getAsInt()));
    }

    @Override
    public ConnectorTableLayoutHandle getAlternativeLayoutHandle(ConnectorSession session, ConnectorTableLayoutHandle tableLayoutHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        HiveTableLayoutHandle hiveLayoutHandle = (HiveTableLayoutHandle) tableLayoutHandle;
        HivePartitioningHandle hivePartitioningHandle = (HivePartitioningHandle) partitioningHandle;

        checkArgument(hiveLayoutHandle.getBucketHandle().isPresent(), "Hive connector only provides alternative layout for bucketed table");
        HiveBucketHandle bucketHandle = hiveLayoutHandle.getBucketHandle().get();
        ImmutableList<HiveType> bucketTypes = bucketHandle.getColumns().stream().map(HiveColumnHandle::getHiveType).collect(toImmutableList());
        Optional<List<HiveType>> hiveTypes = hivePartitioningHandle.getHiveTypes();
        checkArgument(
                hivePartitioningHandle.getBucketFunctionType().equals(HIVE_COMPATIBLE),
                "bucketFunctionType is expected to be HIVE_COMPATIBLE, got: %s",
                hivePartitioningHandle.getBucketFunctionType());
        checkArgument(
                hiveTypes.get().equals(bucketTypes),
                "Types from the new PartitioningHandle (%s) does not match the TableLayoutHandle (%s)",
                hiveTypes.get(),
                bucketTypes);
        int largerBucketCount = Math.max(bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        int smallerBucketCount = Math.min(bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        checkArgument(
                largerBucketCount % smallerBucketCount == 0 && Integer.bitCount(largerBucketCount / smallerBucketCount) == 1,
                "The requested partitioning is not a valid alternative for the table layout");

        HiveBucketHandle updatedBucketHandle = new HiveBucketHandle(bucketHandle.getColumns(), bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        return hiveLayoutHandle.builder()
                .setBucketHandle(Optional.of(updatedBucketHandle))
                .build();
    }

    @Override
    public ConnectorPartitioningHandle getPartitioningHandleForExchange(ConnectorSession session, int partitionCount, List<Type> partitionTypes)
    {
        return getHivePartitionHandle(session, partitionCount, partitionTypes, getBucketFunctionTypeForExchange(session));
    }

    private HivePartitioningHandle getHivePartitionHandle(ConnectorSession session, int partitionCount, List<Type> partitionTypes, BucketFunctionType bucketFunctionType)
    {
        if (isUsePageFileForHiveUnsupportedType(session)) {
            if (!partitionTypes.stream()
                    .allMatch(HiveTypeTranslator::isSupportedHiveType)) {
                bucketFunctionType = PRESTO_NATIVE;
            }
        }
        else if (getTemporaryTableStorageFormat(session) == ORC) {
            // In this case, ORC is either default format or chosen by user. It should not be reset to PAGEFILE.
            // ORC format should not be mixed with PRESTO_NATIVE as ORC compression may reuse PRESTO_NATIVE for compression
            // which leads to hash collision
            bucketFunctionType = HIVE_COMPATIBLE;
        }

        switch (bucketFunctionType) {
            case HIVE_COMPATIBLE:
                return createHiveCompatiblePartitioningHandle(
                        partitionCount,
                        partitionTypes.stream()
                                .map(type -> toHiveType(
                                        typeTranslator,
                                        translateHiveUnsupportedTypeForTemporaryTable(type, typeManager)))
                                .collect(toImmutableList()),
                        OptionalInt.empty());
            case PRESTO_NATIVE:
                return createPrestoNativePartitioningHandle(partitionCount, partitionTypes, OptionalInt.empty());
            default:
                throw new IllegalArgumentException("Unsupported bucket function type " + bucketFunctionType);
        }
    }

    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Table table = metastore.getTable(metastoreContext, hiveTableHandle)
                .orElseThrow(() -> new TableNotFoundException(tableName));

        Optional<HiveBucketHandle> hiveBucketHandle = getHiveBucketHandle(session, table);
        if (!hiveBucketHandle.isPresent()) {
            if (table.getPartitionColumns().isEmpty()) {
                return Optional.empty();
            }

            // TODO: the shuffle partitioning could use a better hash function (instead of Hive bucket function)
            HivePartitioningHandle partitioningHandle = createHiveCompatiblePartitioningHandle(
                    SHUFFLE_MAX_PARALLELISM_FOR_PARTITIONED_TABLE_WRITE,
                    table.getPartitionColumns().stream()
                            .map(Column::getType)
                            .collect(toList()),
                    OptionalInt.empty());
            List<String> partitionedBy = table.getPartitionColumns().stream()
                    .map(Column::getName)
                    .collect(toList());

            return Optional.of(new ConnectorNewTableLayout(
                    partitioningHandle,
                    partitionedBy,
                    isShufflePartitionedColumnsForTableWriteEnabled(session) ? SINGLE_WRITER_PER_PARTITION_REQUIRED : MULTIPLE_WRITERS_PER_PARTITION_ALLOWED));
        }
        HiveBucketProperty bucketProperty = table.getStorage().getBucketProperty()
                .orElseThrow(() -> new NoSuchElementException("Bucket property should be set"));
        if (!bucketProperty.getSortedBy().isEmpty() && !isSortedWritingEnabled(session)) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        HivePartitioningHandle partitioningHandle;
        int bucketCount = hiveBucketHandle.get().getTableBucketCount();
        OptionalInt maxCompatibleBucketCount = OptionalInt.of(bucketCount);
        switch (bucketProperty.getBucketFunctionType()) {
            case HIVE_COMPATIBLE:
                partitioningHandle = createHiveCompatiblePartitioningHandle(
                        bucketCount,
                        hiveBucketHandle.get().getColumns().stream()
                                .map(HiveColumnHandle::getHiveType)
                                .collect(toImmutableList()),
                        maxCompatibleBucketCount);
                break;
            case PRESTO_NATIVE:
                partitioningHandle = createPrestoNativePartitioningHandle(
                        bucketCount,
                        bucketProperty.getTypes().get(),
                        maxCompatibleBucketCount);
                break;
            default:
                throw new IllegalArgumentException("Unsupported bucket function type " + bucketProperty.getBucketFunctionType());
        }

        List<String> partitionColumns = hiveBucketHandle.get().getColumns().stream()
                .map(HiveColumnHandle::getName)
                .collect(toList());
        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitionColumns));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        validateCsvColumns(tableMetadata);
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
            if (partitionedBy.isEmpty()) {
                return Optional.empty();
            }

            List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator);
            Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
            List<Column> partitionColumns = partitionedBy.stream()
                    .map(columnHandlesByName::get)
                    .map(columnHandle -> columnHandleToColumn(session, columnHandle))
                    .collect(toList());

            // TODO: the shuffle partitioning could use a better hash function (instead of Hive bucket function)
            HivePartitioningHandle partitioningHandle = createHiveCompatiblePartitioningHandle(
                    SHUFFLE_MAX_PARALLELISM_FOR_PARTITIONED_TABLE_WRITE,
                    partitionColumns.stream()
                            .map(Column::getType)
                            .collect(toList()),
                    OptionalInt.empty());

            return Optional.of(new ConnectorNewTableLayout(
                    partitioningHandle,
                    partitionedBy,
                    isShufflePartitionedColumnsForTableWriteEnabled(session) ? SINGLE_WRITER_PER_PARTITION_REQUIRED : MULTIPLE_WRITERS_PER_PARTITION_ALLOWED));
        }
        checkArgument(bucketProperty.get().getBucketFunctionType().equals(BucketFunctionType.HIVE_COMPATIBLE),
                "bucketFunctionType is expected to be HIVE_COMPATIBLE, got: %s",
                bucketProperty.get().getBucketFunctionType());
        if (!bucketProperty.get().getSortedBy().isEmpty() && !isSortedWritingEnabled(session)) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        Map<String, HiveType> hiveTypeMap = tableMetadata.getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> toHiveType(typeTranslator, column.getType())));

        return Optional.of(new ConnectorNewTableLayout(
                createHiveCompatiblePartitioningHandle(
                        bucketProperty.get().getBucketCount(),
                        bucketedBy.stream()
                                .map(hiveTypeMap::get)
                                .collect(toImmutableList()),
                        OptionalInt.of(bucketProperty.get().getBucketCount())),
                bucketedBy));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!isCollectColumnStatisticsOnWrite(session)) {
            return TableStatisticsMetadata.empty();
        }
        List<String> partitionedBy = firstNonNull(getPartitionedBy(tableMetadata.getProperties()), ImmutableList.of());
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Optional<Table> table = metastore.getTable(metastoreContext, tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName());
        return getStatisticsCollectionMetadata(session, tableMetadata.getColumns(), partitionedBy, false, table.isPresent() && table.get().getTableType() == TEMPORARY_TABLE);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = firstNonNull(getPartitionedBy(tableMetadata.getProperties()), ImmutableList.of());
        return getStatisticsCollectionMetadata(session, tableMetadata.getColumns(), partitionedBy, true, false);
    }

    private TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, List<ColumnMetadata> columns, List<String> partitionedBy, boolean includeRowCount, boolean isTemporaryTable)
    {
        Set<ColumnStatisticMetadata> columnStatistics = columns.stream()
                .filter(column -> !partitionedBy.contains(column.getName()))
                .filter(column -> !column.isHidden())
                .map(meta -> isTemporaryTable ? this.getColumnStatisticMetadataForTemporaryTable(meta) : this.getColumnStatisticMetadata(session, meta))
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Set<TableStatisticType> tableStatistics = includeRowCount ? ImmutableSet.of(ROW_COUNT) : ImmutableSet.of();
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, partitionedBy);
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(ConnectorSession session, ColumnMetadata columnMetadata)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        return getColumnStatisticMetadata(columnMetadata.getName(), metastore.getSupportedColumnStatistics(metastoreContext, columnMetadata.getType()));
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadataForTemporaryTable(ColumnMetadata columnMetadata)
    {
        return getColumnStatisticMetadata(columnMetadata.getName(), getSupportedColumnStatisticsForTemporaryTable(columnMetadata.getType()));
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(String columnName, Set<ColumnStatisticType> statisticTypes)
    {
        return statisticTypes.stream()
                .map(type -> type.getColumnStatisticMetadata(columnName))
                .collect(toImmutableList());
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<PrestoPrincipal> grantor)
    {
        // roles are case insensitive in Hive
        if (RESERVED_ROLES.contains(role)) {
            throw new PrestoException(ALREADY_EXISTS, "Role name cannot be one of the reserved roles: " + RESERVED_ROLES);
        }
        metastore.createRole(getMetastoreContext(session), role, null);
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        // roles are case insensitive in Hive
        metastore.dropRole(getMetastoreContext(session), role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return ImmutableSet.copyOf(metastore.listRoles(getMetastoreContext(session)));
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, PrestoPrincipal principal)
    {
        return ImmutableSet.copyOf(metastore.listRoleGrants(getMetastoreContext(session), principal));
    }

    @Override
    public void grantRoles(ConnectorSession session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.grantRoles(metastoreContext, roles, grantees, withAdminOption, grantor.orElseGet(() -> new PrestoPrincipal(USER, session.getUser())));
    }

    @Override
    public void revokeRoles(ConnectorSession session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.revokeRoles(metastoreContext, roles, grantees, adminOptionFor, grantor.orElseGet(() -> new PrestoPrincipal(USER, session.getUser())));
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, PrestoPrincipal principal)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        return ThriftMetastoreUtil.listApplicableRoles(principal, (PrestoPrincipal p) -> metastore.listRoleGrants(metastoreContext, p))
                .collect(toImmutableSet());
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        return ThriftMetastoreUtil.listEnabledRoles(session.getIdentity(), (PrestoPrincipal p) -> metastore.listRoleGrants(metastoreContext, p))
                .collect(toImmutableSet());
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Set<HivePrivilegeInfo> hivePrivilegeInfos = privileges.stream()
                .map(privilege -> new HivePrivilegeInfo(toHivePrivilege(privilege), grantOption, new PrestoPrincipal(USER, session.getUser()), new PrestoPrincipal(USER, session.getUser())))
                .collect(toSet());

        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.grantTablePrivileges(metastoreContext, schemaName, tableName, grantee, hivePrivilegeInfos);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Set<HivePrivilegeInfo> hivePrivilegeInfos = privileges.stream()
                .map(privilege -> new HivePrivilegeInfo(toHivePrivilege(privilege), grantOption, new PrestoPrincipal(USER, session.getUser()), new PrestoPrincipal(USER, session.getUser())))
                .collect(toSet());

        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.revokeTablePrivileges(metastoreContext, schemaName, tableName, grantee, hivePrivilegeInfos);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix schemaTablePrefix)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Set<PrestoPrincipal> principals = listEnabledPrincipals(metastore, session.getIdentity(), metastoreContext)
                .collect(toImmutableSet());
        boolean isAdminRoleSet = hasAdminRole(principals);
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        for (SchemaTableName tableName : listTables(session, schemaTablePrefix)) {
            if (isAdminRoleSet) {
                result.addAll(buildGrants(session, tableName, null));
            }
            else {
                for (PrestoPrincipal grantee : principals) {
                    result.addAll(buildGrants(session, tableName, grantee));
                }
            }
        }
        return result.build();
    }

    @Override
    public CompletableFuture<Void> commitPageSinkAsync(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;
        return toCompletableFuture(stagingFileCommitter.commitFiles(
                session,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getLocationHandle().getTargetPath().toString(),
                true,
                getPartitionUpdates(session, fragments)));
    }

    @Override
    public CompletableFuture<Void> commitPageSinkAsync(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) tableHandle;
        return toCompletableFuture(stagingFileCommitter.commitFiles(
                session,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getLocationHandle().getTargetPath().toString(),
                false,
                getPartitionUpdates(session, fragments)));
    }

    private List<GrantInfo> buildGrants(ConnectorSession session, SchemaTableName tableName, PrestoPrincipal principal)
    {
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Set<HivePrivilegeInfo> hivePrivileges = metastore.listTablePrivileges(metastoreContext, tableName.getSchemaName(), tableName.getTableName(), principal);
        for (HivePrivilegeInfo hivePrivilege : hivePrivileges) {
            Set<PrivilegeInfo> prestoPrivileges = hivePrivilege.toPrivilegeInfo();
            for (PrivilegeInfo prestoPrivilege : prestoPrivileges) {
                GrantInfo grant = new GrantInfo(
                        prestoPrivilege,
                        hivePrivilege.getGrantee(),
                        tableName,
                        Optional.of(hivePrivilege.getGrantor()),
                        Optional.empty());
                result.add(grant);
            }
        }
        return result.build();
    }

    private static boolean hasAdminRole(Set<PrestoPrincipal> roles)
    {
        return roles.stream().anyMatch(principal -> principal.getName().equalsIgnoreCase(ADMIN_ROLE_NAME));
    }

    public List<PartitionUpdate> getPartitionUpdates(ConnectorSession session, Collection<Slice> fragments)
    {
        boolean optimizedPartitionUpdateSerializationEnabled = isOptimizedPartitionUpdateSerializationEnabled(session);
        ImmutableList.Builder<PartitionUpdate> result = ImmutableList.builder();
        for (Slice fragment : fragments) {
            byte[] bytes = fragment.getBytes();
            PartitionUpdate partitionUpdate;
            if (optimizedPartitionUpdateSerializationEnabled) {
                partitionUpdate = deserializeZstdCompressed(partitionUpdateSmileCodec, bytes);
            }
            else {
                partitionUpdate = partitionUpdateCodec.fromJson(bytes);
            }
            result.add(partitionUpdate);
        }
        return result.build();
    }

    private void verifyJvmTimeZone()
    {
        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            throw new PrestoException(HIVE_TIMEZONE_MISMATCH, format(
                    "To write Hive data, your JVM timezone must match the Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments.",
                    timeZone.getID()));
        }
    }

    private static HiveStorageFormat extractHiveStorageFormat(Table table)
    {
        StorageFormat storageFormat = table.getStorage().getStorageFormat();
        String outputFormat = storageFormat.getOutputFormat();
        String serde = storageFormat.getSerDe();

        for (HiveStorageFormat format : values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serde)) {
                return format;
            }
        }
        throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serde));
    }

    @VisibleForTesting
    static String encodePreferredOrderingColumns(List<SortingColumn> preferredOrderingColumns)
    {
        return Joiner.on(COMMA).join(preferredOrderingColumns.stream()
                .map(SortingColumn::sortingColumnToString)
                .collect(toImmutableList()));
    }

    @VisibleForTesting
    static List<SortingColumn> decodePreferredOrderingColumnsFromStorage(Storage storage)
    {
        if (!storage.getParameters().containsKey(PREFERRED_ORDERING_COLUMNS)) {
            return ImmutableList.of();
        }

        return Splitter.on(COMMA).trimResults().omitEmptyStrings().splitToList(storage.getParameters().get(PREFERRED_ORDERING_COLUMNS)).stream()
                .map(SortingColumn::sortingColumnFromString)
                .collect(toImmutableList());
    }

    private static void validateBucketColumns(ConnectorTableMetadata tableMetadata)
    {
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            return;
        }
        Set<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toSet());

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        if (!allColumns.containsAll(bucketedBy)) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Bucketing columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(bucketedBy), ImmutableSet.copyOf(allColumns))));
        }

        List<String> sortedBy = bucketProperty.get().getSortedBy().stream()
                .map(SortingColumn::getColumnName)
                .collect(toImmutableList());
        if (!allColumns.containsAll(sortedBy)) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Sorting columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(sortedBy), ImmutableSet.copyOf(allColumns))));
        }
    }

    private static Column columnHandleToColumn(MetastoreContext metastoreContext, HiveColumnHandle handle)
    {
        return new Column(
                handle.getName(),
                handle.getHiveType(),
                handle.getComment(),
                metastoreContext.getColumnConverter().getTypeMetadata(handle.getHiveType(), handle.getTypeSignature()));
    }

    private static void validatePartitionColumns(ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        List<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toList());

        if (!allColumns.containsAll(partitionedBy)) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Partition columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(partitionedBy), ImmutableSet.copyOf(allColumns))));
        }

        if (allColumns.size() == partitionedBy.size()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Table contains only partition columns");
        }

        if (!allColumns.subList(allColumns.size() - partitionedBy.size(), allColumns.size()).equals(partitionedBy)) {
            throw new PrestoException(HIVE_COLUMN_ORDER_MISMATCH, "Partition keys must be the last columns in the table and in the same order as the table properties: " + partitionedBy);
        }
    }

    protected Optional<TableEncryptionProperties> getTableEncryptionPropertiesFromTableProperties(ConnectorTableMetadata tableMetadata, HiveStorageFormat hiveStorageFormat, List<String> partitionedBy)
    {
        ColumnEncryptionInformation columnEncryptionInformation = getEncryptColumns(tableMetadata.getProperties());
        String tableEncryptionReference = getEncryptTable(tableMetadata.getProperties());

        if (tableEncryptionReference == null && (columnEncryptionInformation == null || !columnEncryptionInformation.hasEntries())) {
            return Optional.empty();
        }

        if (tableEncryptionReference != null && columnEncryptionInformation.hasEntries()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Only one of %s or %s should be specified", ENCRYPT_TABLE, ENCRYPT_COLUMNS));
        }

        if (hiveStorageFormat != DWRF) {
            throw new PrestoException(NOT_SUPPORTED, "Only DWRF file format supports encryption at this time");
        }

        // Change based on the file format as more file formats might support encryption

        if (tableEncryptionReference != null) {
            return Optional.of(getDwrfTableEncryptionProperties(Optional.of(tableEncryptionReference), Optional.empty(), tableMetadata));
        }

        partitionedBy.forEach(partitionColumn -> {
            if (columnEncryptionInformation.getColumnToKeyReference().containsKey(ColumnWithStructSubfield.valueOf(partitionColumn))) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Partition column (%s) cannot be used as an encryption column", partitionColumn));
            }
        });

        Map<String, ColumnMetadata> columnMetadata = tableMetadata.getColumns().stream().collect(toImmutableMap(ColumnMetadata::getName, identity()));
        // Sorting to ensure that we can find cases of multiple referenceKeys within the same struct chain. Example - a.b and a.b.c
        // By sorting we ensure that we have already seen a.b and can find that when we visit a.b.c
        List<ColumnWithStructSubfield> sortedColumns = new ArrayList<>(columnEncryptionInformation.getColumnToKeyReference().keySet());
        sortedColumns.sort(Comparator.comparing(ColumnWithStructSubfield::toString));

        Set<String> seenColumns = new HashSet<>();
        for (ColumnWithStructSubfield columnWithSubfield : sortedColumns) {
            ColumnMetadata column = columnMetadata.get(columnWithSubfield.getColumnName());
            if (column == null) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("In %s unable to find column %s", ENCRYPT_COLUMNS, columnWithSubfield.getColumnName()));
            }

            if (seenColumns.contains(columnWithSubfield.toString())) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "The same column/subfield cannot have 2 encryption keys");
            }

            if (columnWithSubfield.getSubfieldPath().isPresent()) {
                Iterable<String> subfieldPathFragments = Splitter.on(".").split(columnWithSubfield.getSubfieldPath().get());
                Type columnType = column.getType();
                String parentPath = columnWithSubfield.getColumnName();

                for (String pathFragment : subfieldPathFragments) {
                    if (!(columnType instanceof RowType)) {
                        throw new PrestoException(
                                INVALID_TABLE_PROPERTY,
                                format("In %s subfields declared in %s, but %s has type %s", ENCRYPT_COLUMNS, columnWithSubfield.toString(), column.getName(), column.getType().getDisplayName()));
                    }

                    if (seenColumns.contains(parentPath)) {
                        throw new PrestoException(
                                INVALID_TABLE_PROPERTY,
                                format("For (%s) found a keyReference at a higher level field (%s)", columnWithSubfield.toString(), parentPath));
                    }

                    RowType row = (RowType) columnType;
                    columnType = row.getFields().stream()
                            .filter(f -> f.getName().orElse("").equals(pathFragment))
                            .findAny()
                            .map(RowType.Field::getType)
                            .orElseThrow(() -> new PrestoException(
                                    INVALID_TABLE_PROPERTY,
                                    format("In %s subfields declared in %s, but %s has type %s", ENCRYPT_COLUMNS, columnWithSubfield.toString(), column.getName(), column.getType().getDisplayName())));

                    parentPath = format("%s.%s", parentPath, pathFragment);
                }
            }

            seenColumns.add(columnWithSubfield.toString());
        }

        return Optional.of(getDwrfTableEncryptionProperties(Optional.empty(), Optional.of(columnEncryptionInformation), tableMetadata));
    }

    private static DwrfTableEncryptionProperties getDwrfTableEncryptionProperties(
            Optional<String> encryptTable,
            Optional<ColumnEncryptionInformation> columnEncryptionInformation,
            ConnectorTableMetadata tableMetadata)
    {
        String encryptionAlgorithm = getDwrfEncryptionAlgorithm(tableMetadata.getProperties());
        String encryptionProvider = getDwrfEncryptionProvider(tableMetadata.getProperties());

        if (encryptionAlgorithm == null) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s needs to be provided for DWRF encrypted tables", DWRF_ENCRYPTION_ALGORITHM));
        }

        if (encryptionProvider == null) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s needs to be provided for DWRF encrypted tables", DWRF_ENCRYPTION_PROVIDER));
        }

        return encryptTable
                .map(s -> forTable(s, encryptionAlgorithm, encryptionProvider))
                .orElseGet(() -> forPerColumn(
                        columnEncryptionInformation.orElseThrow(() -> new PrestoException(GENERIC_INTERNAL_ERROR, "columnEncryptionInformation cannot be empty")),
                        encryptionAlgorithm,
                        encryptionProvider));
    }

    private static List<HiveColumnHandle> getColumnHandles(ConnectorTableMetadata tableMetadata, Set<String> partitionColumnNames, TypeTranslator typeTranslator)
    {
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        validateCsvColumns(tableMetadata);
        return getColumnHandles(tableMetadata.getColumns(), partitionColumnNames, typeTranslator);
    }

    private static List<HiveColumnHandle> getColumnHandles(
            List<ColumnMetadata> columns,
            Set<String> partitionColumnNames,
            TypeTranslator typeTranslator)
    {
        return getColumnHandles(columns, partitionColumnNames, typeTranslator, Optional.empty());
    }

    private static List<HiveColumnHandle> getColumnHandles(
            List<ColumnMetadata> columns,
            Set<String> partitionColumnNames,
            TypeTranslator typeTranslator,
            Optional<HiveType> defaultHiveType)
    {
        ImmutableList.Builder<HiveColumnHandle> columnHandles = ImmutableList.builder();
        int ordinal = 0;
        for (ColumnMetadata column : columns) {
            HiveColumnHandle.ColumnType columnType;
            if (partitionColumnNames.contains(column.getName())) {
                columnType = PARTITION_KEY;
            }
            else if (column.isHidden()) {
                columnType = SYNTHESIZED;
            }
            else {
                columnType = REGULAR;
            }
            columnHandles.add(new HiveColumnHandle(
                    column.getName(),
                    toHiveType(typeTranslator, column.getType(), defaultHiveType),
                    column.getType().getTypeSignature(),
                    ordinal,
                    columnType,
                    column.getComment(),
                    Optional.empty()));
            ordinal++;
        }

        return columnHandles.build();
    }

    private static void validateCsvColumns(ConnectorTableMetadata tableMetadata)
    {
        if (getHiveStorageFormat(tableMetadata.getProperties()) != HiveStorageFormat.CSV) {
            return;
        }

        Set<String> partitionedBy = ImmutableSet.copyOf(getPartitionedBy(tableMetadata.getProperties()));
        List<ColumnMetadata> unsupportedColumns = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !partitionedBy.contains(columnMetadata.getName()))
                .filter(columnMetadata -> !columnMetadata.getType().equals(createUnboundedVarcharType()))
                .collect(toImmutableList());

        if (!unsupportedColumns.isEmpty()) {
            String joinedUnsupportedColumns = unsupportedColumns.stream()
                    .map(columnMetadata -> format("%s %s", columnMetadata.getName(), columnMetadata.getType()))
                    .collect(joining(", "));
            throw new PrestoException(NOT_SUPPORTED, "Hive CSV storage format only supports VARCHAR (unbounded). Unsupported columns: " + joinedUnsupportedColumns);
        }
    }

    @VisibleForTesting
    static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table, TypeManager typeManager, ColumnConverter columnConverter, List<String> notNullColumns)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        table.getPartitionColumns().stream().map(Column::getName).forEach(columnNames::add);
        table.getDataColumns().stream().map(Column::getName).forEach(columnNames::add);
        List<String> allColumnNames = columnNames.build();
        if (allColumnNames.size() > Sets.newHashSet(allColumnNames).size()) {
            throw new PrestoException(HIVE_INVALID_METADATA,
                    format("Hive metadata for table %s is invalid: Table descriptor contains duplicate columns", table.getTableName()));
        }

        List<Column> tableColumns = table.getDataColumns();
        ImmutableMap.Builder<String, Optional<String>> builder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Optional<String>> typeMetadataBuilder = ImmutableMap.builder();

        for (Column field : concat(tableColumns, table.getPartitionColumns())) {
            if (field.getComment().isPresent() && !field.getComment().get().equals("from deserializer")) {
                builder.put(field.getName(), field.getComment());
            }
            else {
                builder.put(field.getName(), Optional.empty());
            }
            typeMetadataBuilder.put(field.getName(), field.getTypeMetadata());
        }
        // add hidden columns
        builder.put(PATH_COLUMN_NAME, Optional.empty());
        if (table.getStorage().getBucketProperty().isPresent()) {
            builder.put(BUCKET_COLUMN_NAME, Optional.empty());
        }

        builder.put(FILE_SIZE_COLUMN_NAME, Optional.empty());
        builder.put(FILE_MODIFIED_TIME_COLUMN_NAME, Optional.empty());
        builder.put(ROW_ID_COLUMN_NAME, Optional.empty());

        Map<String, Optional<String>> columnComment = builder.build();

        Map<String, Optional<String>> typeMetadata = typeMetadataBuilder.build();

        return handle -> ColumnMetadata.builder()
                .setName(handle.getName())
                .setType(typeManager.getType(columnConverter.getTypeSignature(handle.getHiveType(), typeMetadata.getOrDefault(handle.getName(), Optional.empty()))))
                .setNullable(!notNullColumns.contains(handle.getName()))
                .setComment(columnComment.get(handle.getName()).orElse(null))
                .setExtraInfo(columnExtraInfo(handle.isPartitionKey()))
                .setHidden(handle.isHidden())
                .setProperties(ImmutableMap.of())
                .build();
    }

    @Override
    public void rollback()
    {
        metastore.rollback();
    }

    @Override
    public ConnectorCommitHandle commit()
    {
        return metastore.commit();
    }

    public static Optional<SchemaTableName> getSourceTableNameFromSystemTable(SchemaTableName tableName)
    {
        return Stream.of(SystemTableHandler.values())
                .filter(handler -> handler.matches(tableName))
                .map(handler -> handler.getSourceTableName(tableName))
                .findAny();
    }

    private static SystemTable createSystemTable(ConnectorTableMetadata metadata, Function<TupleDomain<Integer>, RecordCursor> cursor)
    {
        return new SystemTable()
        {
            @Override
            public Distribution getDistribution()
            {
                return Distribution.SINGLE_COORDINATOR;
            }

            @Override
            public ConnectorTableMetadata getTableMetadata()
            {
                return metadata;
            }

            @Override
            public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
            {
                return cursor.apply(constraint);
            }
        };
    }

    @Override
    public TableLayoutFilterCoverage getTableLayoutFilterCoverage(ConnectorTableLayoutHandle connectorTableLayoutHandle, Set<String> relevantPartitionColumns)
    {
        HiveTableLayoutHandle tableHandle = (HiveTableLayoutHandle) connectorTableLayoutHandle;
        Set<String> relevantColumns = tableHandle.getPartitionColumns().stream()
                .map(BaseHiveColumnHandle::getName)
                .filter(relevantPartitionColumns::contains)
                .collect(toImmutableSet());
        if (relevantColumns.isEmpty()) {
            return NOT_APPLICABLE;
        }

        return Sets.intersection(tableHandle.getPredicateColumns().keySet(), relevantColumns).isEmpty() ? NOT_COVERED : COVERED;
    }

    @Override
    public void dropConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> constraintName, Optional<String> columnName)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        MetastoreContext metastoreContext = getMetastoreContext(session);
        checkArgument((constraintName.isPresent() && !columnName.isPresent()) || (!constraintName.isPresent() && columnName.isPresent()));
        String constraintToDrop;
        if (constraintName.isPresent()) {
            constraintToDrop = constraintName.get();
        }
        else {
            List<TableConstraint<String>> notNullConstraints = metastore.getTableConstraints(metastoreContext, hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName())
                    .stream()
                    .filter(NotNullConstraint.class::isInstance)
                    .filter(constraint -> constraint.getColumns().stream()
                            .findFirst()
                            .orElse("")
                            .equals(columnName.get()))
                    .collect(toImmutableList());
            if (notNullConstraints.isEmpty() || !notNullConstraints.get(0).getName().isPresent()) {
                throw new PrestoException(NOT_FOUND, format("Not Null constraint not found on column %s", columnName.get()));
            }
            constraintToDrop = notNullConstraints.get(0).getName().get();
        }
        metastore.dropConstraint(metastoreContext, hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), constraintToDrop);
    }

    @Override
    public void addConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, TableConstraint<String> tableConstraint)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.addConstraint(metastoreContext, hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), tableConstraint);
    }

    private enum SystemTableHandler
    {
        PARTITIONS, PROPERTIES;

        private final String suffix;

        SystemTableHandler()
        {
            this.suffix = "$" + name().toLowerCase(ENGLISH);
        }

        boolean matches(SchemaTableName table)
        {
            return table.getTableName().endsWith(suffix) &&
                    (table.getTableName().length() > suffix.length());
        }

        SchemaTableName getSourceTableName(SchemaTableName table)
        {
            return new SchemaTableName(
                    table.getSchemaName(),
                    table.getTableName().substring(0, table.getTableName().length() - suffix.length()));
        }
    }

    private static <T> Optional<T> firstNonNullable(T... values)
    {
        for (T value : values) {
            if (value != null) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }
}
