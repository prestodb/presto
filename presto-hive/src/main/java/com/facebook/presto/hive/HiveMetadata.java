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

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.hive.LocationService.WriteInfo;
import com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil;
import com.facebook.presto.hive.statistics.HiveStatisticsProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPushdownFilterResult;
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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveAnalyzeProperties.getPartitionList;
import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.HiveBasicStatistics.createZeroStatistics;
import static com.facebook.presto.hive.HiveBucketHandle.createVirtualBucketHandle;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucketHandle;
import static com.facebook.presto.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.updateRowIdHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_COLUMN_ORDER_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TIMEZONE_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HivePartitionManager.extractPartitionValues;
import static com.facebook.presto.hive.HiveSessionProperties.getCompressionCodec;
import static com.facebook.presto.hive.HiveSessionProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveSessionProperties.getTemporaryTableCompressionCodec;
import static com.facebook.presto.hive.HiveSessionProperties.getTemporaryTableSchema;
import static com.facebook.presto.hive.HiveSessionProperties.getTemporaryTableStorageFormat;
import static com.facebook.presto.hive.HiveSessionProperties.getVirtualBucketCount;
import static com.facebook.presto.hive.HiveSessionProperties.isBucketExecutionEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isCollectColumnStatisticsOnWrite;
import static com.facebook.presto.hive.HiveSessionProperties.isOptimizedMismatchedBucketCount;
import static com.facebook.presto.hive.HiveSessionProperties.isRespectTableFormat;
import static com.facebook.presto.hive.HiveSessionProperties.isSortedWritingEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isStatisticsEnabled;
import static com.facebook.presto.hive.HiveTableProperties.AVRO_SCHEMA_URL;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static com.facebook.presto.hive.HiveTableProperties.ORC_BLOOM_FILTER_FPP;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.getAvroSchemaUrl;
import static com.facebook.presto.hive.HiveTableProperties.getBucketProperty;
import static com.facebook.presto.hive.HiveTableProperties.getExternalLocation;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveTableProperties.getOrcBloomFilterColumns;
import static com.facebook.presto.hive.HiveTableProperties.getOrcBloomFilterFpp;
import static com.facebook.presto.hive.HiveTableProperties.getPartitionedBy;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.toHiveType;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.assignFieldNamesForAnonymousRowColumns;
import static com.facebook.presto.hive.HiveUtil.columnExtraInfo;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.facebook.presto.hive.HiveUtil.toPartitionValues;
import static com.facebook.presto.hive.HiveUtil.verifyPartitionTypeSupported;
import static com.facebook.presto.hive.HiveWriteUtils.checkTableIsWritable;
import static com.facebook.presto.hive.HiveWriteUtils.isWritableType;
import static com.facebook.presto.hive.HiveWriterFactory.getFileExtension;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.APPEND;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.NEW;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.OVERWRITE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.toHivePrivilege;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getProtectMode;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyOnline;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.VIRTUAL_VIEW;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.listEnabledPrincipals;
import static com.facebook.presto.hive.security.SqlStandardAccessControl.ADMIN_ROLE_NAME;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.ADD;
import static com.facebook.presto.hive.util.Statistics.createComputedStatisticsToPartitionMap;
import static com.facebook.presto.hive.util.Statistics.createEmptyPartitionStatistics;
import static com.facebook.presto.hive.util.Statistics.fromComputedStatistics;
import static com.facebook.presto.hive.util.Statistics.reduce;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Streams.stream;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class HiveMetadata
        implements TransactionalMetadata
{
    public static final String PRESTO_VERSION_NAME = "presto_version";
    public static final String PRESTO_QUERY_ID_NAME = "presto_query_id";
    public static final String TABLE_COMMENT = "comment";
    public static final Set<String> RESERVED_ROLES = ImmutableSet.of("all", "default", "none");

    private static final String ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    private static final String ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";

    private static final String PARTITIONS_TABLE_SUFFIX = "$partitions";
    private static final String PRESTO_TEMPORARY_TABLE_NAME_PREFIX = "__presto_temporary_table_";
    public static final String AVRO_SCHEMA_URL_KEY = "avro.schema.url";

    private final boolean allowCorruptWritesForTesting;
    private final SemiTransactionalHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;
    private final TableParameterCodec tableParameterCodec;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final TypeTranslator typeTranslator;
    private final String prestoVersion;
    private final HiveStatisticsProvider hiveStatisticsProvider;
    private final StagingFileCommitter stagingFileCommitter;
    private final ZeroRowFileCreator zeroRowFileCreator;

    public HiveMetadata(
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            boolean allowCorruptWritesForTesting,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            TypeManager typeManager,
            LocationService locationService,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            TypeTranslator typeTranslator,
            String prestoVersion,
            HiveStatisticsProvider hiveStatisticsProvider,
            StagingFileCommitter stagingFileCommitter,
            ZeroRowFileCreator zeroRowFileCreator)
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
        this.tableParameterCodec = requireNonNull(tableParameterCodec, "tableParameterCodec is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.hiveStatisticsProvider = requireNonNull(hiveStatisticsProvider, "hiveStatisticsProvider is null");
        this.stagingFileCommitter = requireNonNull(stagingFileCommitter, "stagingFileCommitter is null");
        this.zeroRowFileCreator = requireNonNull(zeroRowFileCreator, "zeroRowFileCreator is null");
    }

    public SemiTransactionalHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            return null;
        }

        if (isPartitionsSystemTable(tableName)) {
            // We must not allow $partitions table due to how permissions are checked in PartitionsAwareAccessControl.checkCanSelectFromTable()
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, format("Unexpected table %s present in Hive metastore", tableName));
        }

        verifyOnline(tableName, Optional.empty(), getProtectMode(table.get()), table.get().getParameters());
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
        ConnectorTableMetadata tableMetadata = getTableMetadata(handle.getSchemaTableName());
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
        if (isPartitionsSystemTable(tableName)) {
            return getPartitionsSystemTable(session, tableName);
        }
        return Optional.empty();
    }

    private Optional<SystemTable> getPartitionsSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        SchemaTableName sourceTableName = getSourceTableNameForPartitionsTable(tableName);
        HiveTableHandle sourceTableHandle = getTableHandle(session, sourceTableName);

        if (sourceTableHandle == null) {
            return Optional.empty();
        }

        List<HiveColumnHandle> partitionColumns = getPartitionColumns(sourceTableName);
        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }

        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(HiveColumnHandle::getTypeSignature)
                .map(typeManager::getType)
                .collect(toImmutableList());

        List<ColumnMetadata> partitionSystemTableColumns = partitionColumns.stream()
                .map(column -> new ColumnMetadata(
                        column.getName(),
                        typeManager.getType(column.getTypeSignature()),
                        column.getComment().orElse(null),
                        column.isHidden()))
                .collect(toImmutableList());

        Map<Integer, HiveColumnHandle> fieldIdToColumnHandle =
                IntStream.range(0, partitionColumns.size())
                        .boxed()
                        .collect(toImmutableMap(identity(), partitionColumns::get));

        SystemTable partitionsSystemTable = new SystemTable()
        {
            @Override
            public Distribution getDistribution()
            {
                return Distribution.SINGLE_COORDINATOR;
            }

            @Override
            public ConnectorTableMetadata getTableMetadata()
            {
                return new ConnectorTableMetadata(tableName, partitionSystemTableColumns);
            }

            @Override
            public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
            {
                TupleDomain<ColumnHandle> targetTupleDomain = constraint.transform(fieldIdToColumnHandle::get);
                Predicate<Map<ColumnHandle, NullableValue>> targetPredicate = convertToPredicate(targetTupleDomain);
                Constraint<ColumnHandle> targetConstraint = new Constraint<>(targetTupleDomain, targetPredicate);
                Iterable<List<Object>> records = () ->
                        stream(partitionManager.getPartitionsIterator(metastore, sourceTableHandle, targetConstraint))
                                .map(hivePartition ->
                                        IntStream.range(0, partitionColumns.size())
                                                .mapToObj(fieldIdToColumnHandle::get)
                                                .map(columnHandle -> hivePartition.getKeys().get(columnHandle).getValue())
                                                .collect(toList()))
                                .iterator();

                return new InMemoryRecordSet(partitionColumnTypes, records).cursor();
            }
        };
        return Optional.of(partitionsSystemTable);
    }

    private List<HiveColumnHandle> getPartitionColumns(SchemaTableName tableName)
    {
        Table sourceTable = metastore.getTable(tableName.getSchemaName(), tableName.getTableName()).get();
        return getPartitionKeyColumnHandles(sourceTable);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = schemaTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent() || table.get().getTableType().equals(VIRTUAL_VIEW)) {
            throw new TableNotFoundException(tableName);
        }

        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table.get(), typeManager);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(table.get())) {
            columns.add(metadataGetter.apply(columnHandle));
        }

        // External location property
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (table.get().getTableType().equals(EXTERNAL_TABLE)) {
            properties.put(EXTERNAL_LOCATION_PROPERTY, table.get().getStorage().getLocation());
        }

        // Storage format property
        try {
            HiveStorageFormat format = extractHiveStorageFormat(table.get());
            properties.put(STORAGE_FORMAT_PROPERTY, format);
        }
        catch (PrestoException ignored) {
            // todo fail if format is not known
        }

        // Partitioning property
        List<String> partitionedBy = table.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        // Bucket properties
        Optional<HiveBucketProperty> bucketProperty = table.get().getStorage().getBucketProperty();
        if (bucketProperty.isPresent()) {
            properties.put(BUCKET_COUNT_PROPERTY, bucketProperty.get().getBucketCount());
            properties.put(BUCKETED_BY_PROPERTY, bucketProperty.get().getBucketedBy());
            properties.put(SORTED_BY_PROPERTY, bucketProperty.get().getSortedBy());
        }

        // ORC format specific properties
        String orcBloomFilterColumns = table.get().getParameters().get(ORC_BLOOM_FILTER_COLUMNS_KEY);
        if (orcBloomFilterColumns != null) {
            properties.put(ORC_BLOOM_FILTER_COLUMNS, Splitter.on(',').trimResults().omitEmptyStrings().splitToList(orcBloomFilterColumns));
        }
        String orcBloomFilterFfp = table.get().getParameters().get(ORC_BLOOM_FILTER_FPP_KEY);
        if (orcBloomFilterFfp != null) {
            properties.put(ORC_BLOOM_FILTER_FPP, Double.parseDouble(orcBloomFilterFfp));
        }

        // Avro specfic property
        String avroSchemaUrl = table.get().getParameters().get(AVRO_SCHEMA_URL_KEY);
        if (avroSchemaUrl != null) {
            properties.put(AVRO_SCHEMA_URL, avroSchemaUrl);
        }

        // Hook point for extended versions of the Hive Plugin
        properties.putAll(tableParameterCodec.decode(table.get().getParameters()));

        Optional<String> comment = Optional.ofNullable(table.get().getParameters().get(TABLE_COMMENT));

        return new ConnectorTableMetadata(tableName, columns.build(), properties.build(), comment);
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        HiveTableLayoutHandle tableLayoutHandle = (HiveTableLayoutHandle) layoutHandle;
        if (tableLayoutHandle.getPartitions().isPresent()) {
            return Optional.of(new HiveInputInfo(
                    tableLayoutHandle.getPartitions().get().stream()
                            .map(HivePartition::getPartitionId)
                            .collect(Collectors.toList()),
                    false));
        }

        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : metastore.getAllTables(schemaName).orElse(emptyList())) {
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
        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(table.get())) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        return columnHandles.build();
    }

    @SuppressWarnings("TryWithIdenticalCatches")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (HiveViewNotSupportedException e) {
                // view is not supported
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }

        // TODO Adjust statistics to take into account required subfields

        Map<String, ColumnHandle> columns = columnHandles.stream()
                .map(HiveColumnHandle.class::cast)
                .filter(not(HiveColumnHandle::isHidden))
                .collect(toImmutableMap(HiveColumnHandle::getName, Function.identity()));

        Map<String, Type> columnTypes = columns.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> getColumnMetadata(session, tableHandle, entry.getValue()).getType()));
        List<HivePartition> partitions = partitionManager.getPartitions(metastore, tableHandle, constraint, session).getPartitions();
        return hiveStatisticsProvider.getTableStatistics(session, ((HiveTableHandle) tableHandle).getSchemaTableName(), columns, columnTypes, partitions);
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

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Optional<String> location = HiveSchemaProperties.getLocation(properties).map(locationUri -> {
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

        metastore.createDatabase(database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, schemaName).isEmpty() ||
                !listViews(session, schemaName).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        metastore.dropDatabase(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(source, target);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
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
        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, !partitionedBy.isEmpty(), new HdfsContext(session, schemaName, tableName));

        hiveStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        Path targetPath;
        PrestoTableType tableType;
        String externalLocation = getExternalLocation(tableMetadata.getProperties());
        if (externalLocation != null) {
            if (!createsOfNonManagedTablesEnabled) {
                throw new PrestoException(NOT_SUPPORTED, "Cannot create non-managed Hive table");
            }

            tableType = EXTERNAL_TABLE;
            targetPath = getExternalPath(new HdfsContext(session, schemaName, tableName), externalLocation);
        }
        else {
            tableType = MANAGED_TABLE;
            LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName);
            targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
        }

        Table table = buildTableObject(
                session.getQueryId(),
                schemaName,
                tableName,
                session.getUser(),
                columnHandles,
                hiveStorageFormat,
                partitionedBy,
                bucketProperty,
                tableProperties,
                targetPath,
                tableType,
                prestoVersion);
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner());
        HiveBasicStatistics basicStatistics = table.getPartitionColumns().isEmpty() ? createZeroStatistics() : createEmptyStatistics();
        metastore.createTable(
                session,
                table,
                principalPrivileges,
                Optional.empty(),
                ignoreExisting,
                new PartitionStatistics(basicStatistics, ImmutableMap.of()));
    }

    @Override
    public ConnectorTableHandle createTemporaryTable(ConnectorSession session, List<ColumnMetadata> columns, Optional<ConnectorPartitioningMetadata> partitioningMetadata)
    {
        String schemaName = getTemporaryTableSchema(session);
        String tableName = PRESTO_TEMPORARY_TABLE_NAME_PREFIX + randomUUID().toString().replaceAll("-", "_");
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
            return new HiveBucketProperty(partitioning.getPartitionColumns(), partitioningHandle.getBucketCount(), ImmutableList.of());
        });

        List<HiveColumnHandle> columnHandles = getColumnHandles(
                // Hive doesn't support anonymous rows
                // Since this method doesn't create a real table, it is fine to assign dummy field names to the anonymous rows
                assignFieldNamesForAnonymousRowColumns(columns, typeManager),
                ImmutableSet.of(),
                typeTranslator);
        storageFormat.validateColumns(columnHandles);

        Table table = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(session.getUser())
                .setTableType(TEMPORARY_TABLE)
                .setDataColumns(columnHandles.stream()
                        .map(handle -> new Column(handle.getName(), handle.getHiveType(), handle.getComment()))
                        .collect(toImmutableList()))
                .withStorage(storage -> storage
                        .setStorageFormat(fromHiveStorageFormat(storageFormat))
                        .setBucketProperty(bucketProperty)
                        .setLocation(""))
                .build();

        metastore.createTable(
                session,
                table,
                buildInitialPrivilegeSet(table.getOwner()),
                Optional.empty(),
                false,
                new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()));

        return new HiveTableHandle(schemaName, tableName);
    }

    private Map<String, String> getEmptyTableProperties(ConnectorTableMetadata tableMetadata, boolean partitioned, HdfsContext hdfsContext)
    {
        Builder<String, String> tableProperties = ImmutableMap.builder();

        // Hook point for extended versions of the Hive Plugin
        tableProperties.putAll(tableParameterCodec.encode(tableMetadata.getProperties()));

        // ORC format specific properties
        List<String> columns = getOrcBloomFilterColumns(tableMetadata.getProperties());
        if (columns != null && !columns.isEmpty()) {
            tableProperties.put(ORC_BLOOM_FILTER_COLUMNS_KEY, Joiner.on(",").join(columns));
            tableProperties.put(ORC_BLOOM_FILTER_FPP_KEY, String.valueOf(getOrcBloomFilterFpp(tableMetadata.getProperties())));
        }

        // Avro specific properties
        String avroSchemaUrl = getAvroSchemaUrl(tableMetadata.getProperties());
        if (avroSchemaUrl != null) {
            HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
            if (hiveStorageFormat != HiveStorageFormat.AVRO) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Cannot specify %s table property for storage format: %s", AVRO_SCHEMA_URL, hiveStorageFormat));
            }
            tableProperties.put(AVRO_SCHEMA_URL_KEY, validateAndNormalizeAvroSchemaUrl(avroSchemaUrl, hdfsContext));
        }

        // Table comment property
        tableMetadata.getComment().ifPresent(value -> tableProperties.put(TABLE_COMMENT, value));

        return tableProperties.build();
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
            Map<String, String> additionalTableParameters,
            Path targetPath,
            PrestoTableType tableType,
            String prestoVersion)
    {
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
                .collect(toList());

        Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            HiveType type = columnHandle.getHiveType();
            if (!partitionColumnNames.contains(name)) {
                verify(!columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
                columns.add(new Column(name, type, columnHandle.getComment()));
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
                .setLocation(targetPath.toString());

        return tableBuilder.build();
    }

    private static PrincipalPrivileges buildInitialPrivilegeSet(String tableOwner)
    {
        PrestoPrincipal owner = new PrestoPrincipal(USER, tableOwner);
        return new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.SELECT, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.INSERT, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.UPDATE, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilege.DELETE, true, owner, owner))
                        .build(),
                ImmutableMultimap.of());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(handle);

        metastore.addColumn(handle.getSchemaName(), handle.getTableName(), column.getName(), toHiveType(typeTranslator, column.getType()), column.getComment());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(hiveTableHandle);
        HiveColumnHandle sourceHandle = (HiveColumnHandle) source;

        metastore.renameColumn(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), sourceHandle.getName(), target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(hiveTableHandle);
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;

        metastore.dropColumn(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), columnHandle.getName());
    }

    private void failIfAvroSchemaIsSet(HiveTableHandle handle)
    {
        String tableName = handle.getTableName();
        String schemaName = handle.getSchemaName();
        Optional<Table> table = metastore.getTable(schemaName, tableName);

        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(schemaName, tableName));
        }

        if (table.get().getParameters().get(AVRO_SCHEMA_URL_KEY) != null) {
            throw new PrestoException(NOT_SUPPORTED, "ALTER TABLE not supported when Avro schema url is set");
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = schemaTableName(tableHandle);

        Optional<Table> target = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        verifyJvmTimeZone();
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = handle.getSchemaTableName();

        metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        return handle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = handle.getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
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
            metastore.setTableStatistics(table, createPartitionStatistics(session, columnTypes, computedStatisticsMap.get(ImmutableList.<String>of())));
        }
        else {
            List<List<String>> partitionValuesList;
            if (handle.getAnalyzePartitionValues().isPresent()) {
                partitionValuesList = handle.getAnalyzePartitionValues().get();
            }
            else {
                partitionValuesList = metastore.getPartitionNames(handle.getSchemaName(), handle.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(((HiveTableHandle) tableHandle).getSchemaTableName()))
                        .stream()
                        .map(HiveUtil::toPartitionValues)
                        .collect(toImmutableList());
            }

            ImmutableMap.Builder<List<String>, PartitionStatistics> partitionStatistics = ImmutableMap.builder();
            Map<String, Set<ColumnStatisticType>> columnStatisticTypes = hiveColumnHandles.stream()
                    .filter(columnHandle -> !partitionColumnNames.contains(columnHandle.getName()))
                    .filter(column -> !column.isHidden())
                    .collect(toImmutableMap(HiveColumnHandle::getName, column -> ImmutableSet.copyOf(metastore.getSupportedColumnStatistics(typeManager.getType(column.getTypeSignature())))));
            Supplier<PartitionStatistics> emptyPartitionStatistics = Suppliers.memoize(() -> createEmptyPartitionStatistics(columnTypes, columnStatisticTypes));

            int usedComputedStatistics = 0;
            for (List<String> partitionValues : partitionValuesList) {
                ComputedStatistics collectedStatistics = computedStatisticsMap.get(partitionValues);
                if (collectedStatistics == null) {
                    partitionStatistics.put(partitionValues, emptyPartitionStatistics.get());
                }
                else {
                    usedComputedStatistics++;
                    partitionStatistics.put(partitionValues, createPartitionStatistics(session, columnTypes, collectedStatistics));
                }
            }
            verify(usedComputedStatistics == computedStatistics.size(), "All computed statistics must be used");
            metastore.setPartitionStatistics(table, partitionStatistics.build());
        }
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

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, !partitionedBy.isEmpty(), new HdfsContext(session, schemaName, tableName));
        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator);
        HiveStorageFormat partitionStorageFormat = isRespectTableFormat(session) ? tableStorageFormat : getHiveStorageFormat(session);

        // unpartitioned tables ignore the partition storage format
        HiveStorageFormat actualStorageFormat = partitionedBy.isEmpty() ? tableStorageFormat : partitionStorageFormat;
        actualStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName);
        HiveOutputTableHandle result = new HiveOutputTableHandle(
                schemaName,
                tableName,
                columnHandles,
                session.getQueryId(),
                metastore.generatePageSinkMetadata(schemaTableName),
                locationHandle,
                tableStorageFormat,
                partitionStorageFormat,
                getCompressionCodec(session),
                partitionedBy,
                bucketProperty,
                session.getUser(),
                tableProperties);

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), result.getFilePrefix(), schemaTableName, false);

        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;

        List<PartitionUpdate> partitionUpdates = getPartitionUpdates(fragments);

        WriteInfo writeInfo = locationService.getQueryWriteInfo(handle.getLocationHandle());
        Table table = buildTableObject(
                session.getQueryId(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableOwner(),
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionedBy(),
                handle.getBucketProperty(),
                handle.getAdditionalTableParameters(),
                writeInfo.getTargetPath(),
                MANAGED_TABLE,
                prestoVersion);
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(handle.getTableOwner());

        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        if (handle.getBucketProperty().isPresent()) {
            ImmutableList<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(session, handle, table, partitionUpdates);
            // replace partitionUpdates before creating the zero-row files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(Iterables.concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            HdfsContext hdfsContext = new HdfsContext(session, table.getDatabaseName(), table.getTableName());
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session, table, partitionUpdate));
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
                    .orElse(createZeroStatistics());
            tableStatistics = createPartitionStatistics(session, basicStatistics, columnTypes, getColumnStatistics(partitionComputedStatistics, ImmutableList.of()));
        }
        else {
            tableStatistics = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }

        metastore.createTable(session, table, principalPrivileges, Optional.of(writeInfo.getWritePath()), false, tableStatistics);

        if (handle.getPartitionedBy().isEmpty()) {
            return Optional.of(new HiveWrittenPartitions(ImmutableList.of(UNPARTITIONED_ID)));
        }

        if (isRespectTableFormat(session)) {
            Verify.verify(handle.getPartitionStorageFormat() == handle.getTableStorageFormat());
        }
        for (PartitionUpdate update : partitionUpdates) {
            Partition partition = buildPartitionObject(session, table, update);
            PartitionStatistics partitionStatistics = createPartitionStatistics(
                    session,
                    update.getStatistics(),
                    columnTypes,
                    getColumnStatistics(partitionComputedStatistics, partition.getValues()));
            metastore.addPartition(
                    session,
                    handle.getSchemaName(),
                    handle.getTableName(),
                    buildPartitionObject(session, table, update),
                    update.getWritePath(),
                    partitionStatistics);
        }

        return Optional.of(new HiveWrittenPartitions(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .collect(Collectors.toList())));
    }

    private Properties getSchema(Optional<Partition> partition, Table table)
    {
        return partition.isPresent() ? getHiveSchema(partition.get(), table) : getHiveSchema(table);
    }

    private StorageFormat getStorageFormat(Optional<Partition> partition, Table table)
    {
        return partition.isPresent() ? partition.get().getStorage().getStorageFormat() : table.getStorage().getStorageFormat();
    }

    private ImmutableList<PartitionUpdate> computePartitionUpdatesForMissingBuckets(
            ConnectorSession session,
            HiveWritableTableHandle handle,
            Table table,
            List<PartitionUpdate> partitionUpdates)
    {
        HiveStorageFormat storageFormat = table.getPartitionColumns().isEmpty() ? handle.getTableStorageFormat() : handle.getPartitionStorageFormat();

        // empty unpartitioned bucketed table
        if (table.getPartitionColumns().isEmpty() && partitionUpdates.isEmpty()) {
            int bucketCount = handle.getBucketProperty().get().getBucketCount();
            LocationHandle locationHandle = handle.getLocationHandle();
            List<String> fileNamesForMissingBuckets = computeFileNamesForMissingBuckets(
                    storageFormat,
                    handle.getCompressionCodec(),
                    handle.getFilePrefix(),
                    bucketCount,
                    ImmutableSet.of());
            return ImmutableList.of(new PartitionUpdate(
                    "",
                    (handle instanceof HiveInsertTableHandle) ? APPEND : NEW,
                    locationHandle.getWritePath(),
                    locationHandle.getTargetPath(),
                    fileNamesForMissingBuckets.stream()
                            .map(fileName -> new FileWriteInfo(fileName, fileName))
                            .collect(toImmutableList()),
                    0,
                    0,
                    0));
        }

        ImmutableList.Builder<PartitionUpdate> partitionUpdatesForMissingBucketsBuilder = ImmutableList.builder();
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            int bucketCount = handle.getBucketProperty().get().getBucketCount();

            List<String> fileNamesForMissingBuckets = computeFileNamesForMissingBuckets(
                    storageFormat,
                    handle.getCompressionCodec(),
                    handle.getFilePrefix(),
                    bucketCount,
                    ImmutableSet.copyOf(getTargetFileNames(partitionUpdate.getFileWriteInfos())));
            partitionUpdatesForMissingBucketsBuilder.add(new PartitionUpdate(
                    partitionUpdate.getName(),
                    partitionUpdate.getUpdateMode(),
                    partitionUpdate.getWritePath(),
                    partitionUpdate.getTargetPath(),
                    fileNamesForMissingBuckets.stream()
                            .map(fileName -> new FileWriteInfo(fileName, fileName))
                            .collect(toImmutableList()),
                    0,
                    0,
                    0));
        }
        return partitionUpdatesForMissingBucketsBuilder.build();
    }

    private List<String> computeFileNamesForMissingBuckets(
            HiveStorageFormat storageFormat,
            HiveCompressionCodec compressionCodec,
            String filePrefix,
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
            String targetFileName = HiveWriterFactory.computeBucketedFileName(filePrefix, i) + fileExtension;
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
        verifyJvmTimeZone();

        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }

        checkTableIsWritable(table.get(), writesToNonManagedTablesEnabled);

        for (Column column : table.get().getDataColumns()) {
            if (!isWritableType(column.getType())) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("Inserting into Hive table %s.%s with column type %s not supported", table.get().getDatabaseName(), table.get().getTableName(), column.getType()));
            }
        }

        List<HiveColumnHandle> handles = hiveColumnHandles(table.get()).stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toList());

        HiveStorageFormat tableStorageFormat = extractHiveStorageFormat(table.get());
        LocationHandle locationHandle;
        boolean isTemporaryTable = table.get().getTableType().equals(TEMPORARY_TABLE);
        if (isTemporaryTable) {
            locationHandle = locationService.forTemporaryTable(metastore, session, table.get());
        }
        else {
            locationHandle = locationService.forExistingTable(metastore, session, table.get());
        }
        HiveInsertTableHandle result = new HiveInsertTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                handles,
                session.getQueryId(),
                metastore.generatePageSinkMetadata(tableName),
                locationHandle,
                table.get().getStorage().getBucketProperty(),
                tableStorageFormat,
                isRespectTableFormat(session) ? tableStorageFormat : HiveSessionProperties.getHiveStorageFormat(session),
                isTemporaryTable ? getTemporaryTableCompressionCodec(session) : getCompressionCodec(session));

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), result.getFilePrefix(), tableName, isTemporaryTable);
        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) insertHandle;

        List<PartitionUpdate> partitionUpdates = getPartitionUpdates(fragments);

        HiveStorageFormat tableStorageFormat = handle.getTableStorageFormat();
        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
        }
        if (!table.get().getStorage().getStorageFormat().getInputFormat().equals(tableStorageFormat.getInputFormat()) && isRespectTableFormat(session)) {
            throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during insert");
        }

        if (handle.getBucketProperty().isPresent()) {
            ImmutableList<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(session, handle, table.get(), partitionUpdates);
            // replace partitionUpdates before creating the zero-row files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(Iterables.concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            HdfsContext hdfsContext = new HdfsContext(session, table.get().getDatabaseName(), table.get().getTableName());
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.get().getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session, table.get(), partitionUpdate));
                zeroRowFileCreator.createFiles(
                        session,
                        hdfsContext,
                        partitionUpdate.getWritePath(),
                        getTargetFileNames(partitionUpdate.getFileWriteInfos()),
                        getStorageFormat(partition, table.get()),
                        handle.getCompressionCodec(),
                        getSchema(partition, table.get()));
            }
        }

        List<String> partitionedBy = table.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = createComputedStatisticsToPartitionMap(computedStatistics, partitionedBy, columnTypes);

        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            if (partitionUpdate.getName().isEmpty()) {
                // insert into unpartitioned table
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        session,
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, ImmutableList.of()));
                metastore.finishInsertIntoExistingTable(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionUpdate.getWritePath(),
                        getTargetFileNames(partitionUpdate.getFileWriteInfos()),
                        partitionStatistics);
            }
            else if (partitionUpdate.getUpdateMode() == APPEND) {
                // insert into existing partition
                List<String> partitionValues = toPartitionValues(partitionUpdate.getName());
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        session,
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partitionValues));
                metastore.finishInsertIntoExistingPartition(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionValues,
                        partitionUpdate.getWritePath(),
                        getTargetFileNames(partitionUpdate.getFileWriteInfos()),
                        partitionStatistics);
            }
            else if (partitionUpdate.getUpdateMode() == NEW || partitionUpdate.getUpdateMode() == OVERWRITE) {
                // insert into new partition or overwrite existing partition
                Partition partition = buildPartitionObject(session, table.get(), partitionUpdate);
                if (!partition.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && isRespectTableFormat(session)) {
                    throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Partition format changed during insert");
                }
                if (partitionUpdate.getUpdateMode() == OVERWRITE) {
                    metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), partition.getValues());
                }
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        session,
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partition.getValues()));
                metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition, partitionUpdate.getWritePath(), partitionStatistics);
            }
            else {
                throw new IllegalArgumentException(format("Unsupported update mode: %s", partitionUpdate.getUpdateMode()));
            }
        }

        return Optional.of(new HiveWrittenPartitions(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .map(name -> name.isEmpty() ? UNPARTITIONED_ID : name)
                        .collect(Collectors.toList())));
    }

    private List<String> getTargetFileNames(List<FileWriteInfo> fileWriteInfos)
    {
        return fileWriteInfos.stream()
                .map(FileWriteInfo::getTargetFileName)
                .collect(toImmutableList());
    }

    private Partition buildPartitionObject(ConnectorSession session, Table table, PartitionUpdate partitionUpdate)
    {
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(extractPartitionValues(partitionUpdate.getName()))
                .setParameters(ImmutableMap.<String, String>builder()
                        .put(PRESTO_VERSION_NAME, prestoVersion)
                        .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                        .build())
                .withStorage(storage -> storage
                        .setStorageFormat(isRespectTableFormat(session) ?
                                table.getStorage().getStorageFormat() :
                                fromHiveStorageFormat(HiveSessionProperties.getHiveStorageFormat(session)))
                        .setLocation(partitionUpdate.getTargetPath().toString())
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }

    private PartitionStatistics createPartitionStatistics(
            ConnectorSession session,
            Map<String, Type> columnTypes,
            ComputedStatistics computedStatistics)
    {
        Map<ColumnStatisticMetadata, Block> computedColumnStatistics = computedStatistics.getColumnStatistics();

        Block rowCountBlock = Optional.ofNullable(computedStatistics.getTableStatistics().get(ROW_COUNT))
                .orElseThrow(() -> new VerifyException("rowCount not present"));
        verify(!rowCountBlock.isNull(0), "rowCount must never be null");
        long rowCount = BIGINT.getLong(rowCountBlock, 0);
        HiveBasicStatistics rowCountOnlyBasicStatistics = new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(rowCount), OptionalLong.empty(), OptionalLong.empty());
        return createPartitionStatistics(session, rowCountOnlyBasicStatistics, columnTypes, computedColumnStatistics);
    }

    private PartitionStatistics createPartitionStatistics(
            ConnectorSession session,
            HiveBasicStatistics basicStatistics,
            Map<String, Type> columnTypes,
            Map<ColumnStatisticMetadata, Block> computedColumnStatistics)
    {
        long rowCount = basicStatistics.getRowCount().orElseThrow(() -> new IllegalArgumentException("rowCount not present"));
        Map<String, HiveColumnStatistics> columnStatistics = fromComputedStatistics(
                session,
                timeZone,
                computedColumnStatistics,
                columnTypes,
                rowCount);
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    private Map<ColumnStatisticMetadata, Block> getColumnStatistics(Map<List<String>, ComputedStatistics> partitionComputedStatistics, List<String> partitionValues)
    {
        return Optional.ofNullable(partitionComputedStatistics.get(partitionValues))
                .map(ComputedStatistics::getColumnStatistics)
                .orElse(ImmutableMap.of());
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(TABLE_COMMENT, "Presto View")
                .put(PRESTO_VIEW_FLAG, "true")
                .put(PRESTO_VERSION_NAME, prestoVersion)
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .build();

        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(session.getUser())
                .setTableType(VIRTUAL_VIEW)
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(properties)
                .setViewOriginalText(Optional.of(encodeViewData(viewData)))
                .setViewExpandedText(Optional.of("/* Presto View */"));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(VIEW_STORAGE_FORMAT)
                .setLocation("");
        Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(session.getUser());

        Optional<Table> existing = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !HiveUtil.isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(viewName);
            }

            metastore.replaceView(viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(session, table, principalPrivileges, Optional.empty(), false, new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()));
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorViewDefinition view = getViews(session, viewName.toSchemaTablePrefix()).get(viewName);
        if (view == null) {
            throw new ViewNotFoundException(viewName);
        }

        try {
            metastore.dropTable(session, viewName.getSchemaName(), viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : metastore.getAllViews(schemaName).orElse(emptyList())) {
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

        for (SchemaTableName schemaTableName : tableNames) {
            Optional<Table> table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (table.isPresent() && HiveUtil.isPrestoView(table.get())) {
                views.put(schemaTableName, new ConnectorViewDefinition(
                        schemaTableName,
                        Optional.ofNullable(table.get().getOwner()),
                        decodeViewData(table.get().getViewOriginalText().get())));
            }
        }

        return views.build();
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely");
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return updateRowIdHandle();
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableLayoutHandle;

        Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        if (table.get().getPartitionColumns().isEmpty()) {
            metastore.truncateUnpartitionedTable(session, handle.getSchemaName(), handle.getTableName());
        }
        else {
            for (HivePartition hivePartition : getOrComputePartitions(layoutHandle, session, tableHandle)) {
                metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), toPartitionValues(hivePartition.getPartitionId()));
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
            List<ConnectorTableLayoutResult> tableLayoutResults = getTableLayouts(session, tableHandle, new Constraint<>(partitionColumnPredicate, predicate), Optional.empty());
            return ((HiveTableLayoutHandle) Iterables.getOnlyElement(tableLayoutResults).getTableLayout().getHandle()).getPartitions().get();
        }
    }

    @VisibleForTesting
    static Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> tupleDomain)
    {
        return bindings -> tupleDomain.contains(TupleDomain.fromFixedValues(bindings));
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return true;
    }

    @Override
    public boolean isPushdownFilterSupported(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (((HiveTableHandle) tableHandle).getAnalyzePartitionValues().isPresent()) {
            return false;
        }

        return isPushdownFilterEnabled(session, tableHandle);
    }

    @Override
    public ConnectorPushdownFilterResult pushdownFilter(ConnectorSession session, ConnectorTableHandle tableHandle, RowExpression filter, Optional<ConnectorTableLayoutHandle> currentLayoutHandle)
    {
        if (TRUE_CONSTANT.equals(filter) && currentLayoutHandle.isPresent()) {
            return new ConnectorPushdownFilterResult(getTableLayout(session, currentLayoutHandle.get()), TRUE_CONSTANT);
        }

        if (currentLayoutHandle.isPresent()) {
            throw new UnsupportedOperationException("Partial filter pushdown is not supported");
        }

        // Split the filter into 3 groups of conjuncts:
        //  - range filters that apply to entire columns,
        //  - range filters that apply to subfields,
        //  - the rest
        ExtractionResult<Subfield> decomposedFilter = rowExpressionService.getDomainTranslator()
                .fromPredicate(session, filter, new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session));

        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);
        TupleDomain<ColumnHandle> entireColumnDomain = decomposedFilter.getTupleDomain()
                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                .transform(columnHandles::get);
        // TODO Extract deterministic conjuncts that apply to partition columns and specify these as Contraint#predicate
        HivePartitionResult hivePartitionResult = partitionManager.getPartitions(metastore, tableHandle, new Constraint<>(entireColumnDomain), session);

        TupleDomain<Subfield> domainPredicate = withColumnDomains(ImmutableMap.<Subfield, Domain>builder()
                .putAll(hivePartitionResult.getUnenforcedConstraint()
                        .transform(HiveMetadata::toSubfield)
                        .getDomains()
                        .orElse(ImmutableMap.of()))
                .putAll(decomposedFilter.getTupleDomain()
                        .transform(subfield -> !isEntireColumn(subfield) ? subfield : null)
                        .getDomains()
                        .orElse(ImmutableMap.of()))
                .build());

        Set<String> predicateColumnNames = new HashSet<>();
        domainPredicate.getDomains().get().keySet().stream()
                .map(Subfield::getRootName)
                .forEach(predicateColumnNames::add);
        extractAll(decomposedFilter.getRemainingExpression()).stream()
                .map(VariableReferenceExpression::getName)
                .forEach(predicateColumnNames::add);

        Map<String, HiveColumnHandle> predicateColumns = predicateColumnNames.stream()
                .map(columnHandles::get)
                .map(HiveColumnHandle.class::cast)
                .collect(toImmutableMap(HiveColumnHandle::getName, Functions.identity()));

        return new ConnectorPushdownFilterResult(
                getTableLayout(
                        session,
                        new HiveTableLayoutHandle(
                                ((HiveTableHandle) tableHandle).getSchemaTableName(),
                                ImmutableList.copyOf(hivePartitionResult.getPartitionColumns()),
                                hivePartitionResult.getPartitions(),
                                domainPredicate,
                                decomposedFilter.getRemainingExpression(),
                                predicateColumns,
                                hivePartitionResult.getEnforcedConstraint(),
                                hivePartitionResult.getBucketHandle(),
                                hivePartitionResult.getBucketFilter())),
                TRUE_CONSTANT);
    }

    private static Set<VariableReferenceExpression> extractAll(RowExpression expression)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new VariableReferenceBuilderVisitor(), builder);
        return builder.build();
    }

    private static class VariableReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(VariableReferenceExpression variable, ImmutableSet.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
            return null;
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
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

        return ImmutableList.of(new ConnectorTableLayoutResult(
                getTableLayout(
                        session,
                        new HiveTableLayoutHandle(
                                handle.getSchemaTableName(),
                                ImmutableList.copyOf(hivePartitionResult.getPartitionColumns()),
                                hivePartitionResult.getPartitions(),
                                hivePartitionResult.getEffectivePredicate().transform(HiveMetadata::toSubfield),
                                TRUE_CONSTANT,
                                predicateColumns,
                                hivePartitionResult.getEnforcedConstraint(),
                                hiveBucketHandle,
                                hivePartitionResult.getBucketFilter())),
                hivePartitionResult.getUnenforcedConstraint()));
    }

    private static Subfield toSubfield(ColumnHandle columnHandle)
    {
        return new Subfield(((HiveColumnHandle) columnHandle).getName(), ImmutableList.of());
    }

    private static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    private boolean isPushdownFilterEnabled(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        boolean pushdownFilterEnabled = HiveSessionProperties.isPushdownFilterEnabled(session);
        if (pushdownFilterEnabled) {
            HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(getTableMetadata(session, tableHandle).getProperties());
            if (hiveStorageFormat == HiveStorageFormat.ORC || hiveStorageFormat == HiveStorageFormat.DWRF) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        HiveTableLayoutHandle hiveLayoutHandle = (HiveTableLayoutHandle) layoutHandle;
        List<ColumnHandle> partitionColumns = hiveLayoutHandle.getPartitionColumns();
        List<HivePartition> partitions = hiveLayoutHandle.getPartitions().get();

        TupleDomain<ColumnHandle> predicate = createPredicate(partitionColumns, partitions);

        Optional<DiscretePredicates> discretePredicates = Optional.empty();
        if (!partitionColumns.isEmpty()) {
            // Do not create tuple domains for every partition at the same time!
            // There can be a huge number of partitions so use an iterable so
            // all domains do not need to be in memory at the same time.
            Iterable<TupleDomain<ColumnHandle>> partitionDomains = Iterables.transform(partitions, (hivePartition) -> TupleDomain.fromFixedValues(hivePartition.getKeys()));
            discretePredicates = Optional.of(new DiscretePredicates(partitionColumns, partitionDomains));
        }

        Optional<ConnectorTablePartitioning> tablePartitioning = Optional.empty();
        SchemaTableName tableName = hiveLayoutHandle.getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        // never ignore table bucketing for temporary tables as those are created such explicitly by the engine request
        boolean bucketExecutionEnabled = table.getTableType().equals(TEMPORARY_TABLE) || isBucketExecutionEnabled(session);
        if (bucketExecutionEnabled && hiveLayoutHandle.getBucketHandle().isPresent()) {
            tablePartitioning = hiveLayoutHandle.getBucketHandle().map(hiveBucketHandle -> new ConnectorTablePartitioning(
                    new HivePartitioningHandle(
                            hiveBucketHandle.getReadBucketCount(),
                            hiveBucketHandle.getColumns().stream()
                                    .map(HiveColumnHandle::getHiveType)
                                    .collect(Collectors.toList()),
                            OptionalInt.empty()),
                    hiveBucketHandle.getColumns().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toList())));
        }

        return new ConnectorTableLayout(
                hiveLayoutHandle,
                Optional.empty(),
                predicate,
                tablePartitioning,
                Optional.empty(),
                discretePredicates,
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        HivePartitioningHandle leftHandle = (HivePartitioningHandle) left;
        HivePartitioningHandle rightHandle = (HivePartitioningHandle) right;

        if (!leftHandle.getHiveTypes().equals(rightHandle.getHiveTypes())) {
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
                leftHandle.getHiveTypes(),
                maxCompatibleBucketCount));
    }

    @Override
    public boolean isRefinedPartitioningOver(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        HivePartitioningHandle leftHandle = (HivePartitioningHandle) left;
        HivePartitioningHandle rightHandle = (HivePartitioningHandle) right;

        if (!leftHandle.getHiveTypes().equals(rightHandle.getHiveTypes())) {
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
        checkArgument(
                hivePartitioningHandle.getHiveTypes().equals(bucketTypes),
                "Types from the new PartitioningHandle (%s) does not match the TableLayoutHandle (%s)",
                hivePartitioningHandle.getHiveTypes(),
                bucketTypes);
        int largerBucketCount = Math.max(bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        int smallerBucketCount = Math.min(bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        checkArgument(
                largerBucketCount % smallerBucketCount == 0 && Integer.bitCount(largerBucketCount / smallerBucketCount) == 1,
                "The requested partitioning is not a valid alternative for the table layout");

        return new HiveTableLayoutHandle(
                hiveLayoutHandle.getSchemaTableName(),
                hiveLayoutHandle.getPartitionColumns(),
                hiveLayoutHandle.getPartitions().get(),
                hiveLayoutHandle.getDomainPredicate(),
                hiveLayoutHandle.getRemainingPredicate(),
                hiveLayoutHandle.getPredicateColumns(),
                hiveLayoutHandle.getPartitionColumnPredicate(),
                Optional.of(new HiveBucketHandle(bucketHandle.getColumns(), bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount())),
                hiveLayoutHandle.getBucketFilter());
    }

    @Override
    public ConnectorPartitioningHandle getPartitioningHandleForExchange(ConnectorSession session, int partitionCount, List<Type> partitionTypes)
    {
        return new HivePartitioningHandle(
                partitionCount,
                partitionTypes.stream()
                        .map(type -> toHiveType(typeTranslator, type))
                        .collect(toImmutableList()),
                OptionalInt.empty());
    }

    @VisibleForTesting
    static TupleDomain<ColumnHandle> createPredicate(List<ColumnHandle> partitionColumns, List<HivePartition> partitions)
    {
        if (partitions.isEmpty()) {
            return TupleDomain.none();
        }

        return withColumnDomains(
                partitionColumns.stream()
                        .collect(Collectors.toMap(
                                identity(),
                                column -> buildColumnDomain(column, partitions))));
    }

    private static Domain buildColumnDomain(ColumnHandle column, List<HivePartition> partitions)
    {
        checkArgument(!partitions.isEmpty(), "partitions cannot be empty");

        boolean hasNull = false;
        List<Object> nonNullValues = new ArrayList<>();
        Type type = null;

        for (HivePartition partition : partitions) {
            NullableValue value = partition.getKeys().get(column);
            if (value == null) {
                throw new PrestoException(HIVE_UNKNOWN_ERROR, format("Partition %s does not have a value for partition column %s", partition, column));
            }

            if (value.isNull()) {
                hasNull = true;
            }
            else {
                nonNullValues.add(value.getValue());
            }

            if (type == null) {
                type = value.getType();
            }
        }

        if (!nonNullValues.isEmpty()) {
            Domain domain = Domain.multipleValues(type, nonNullValues);
            if (hasNull) {
                return domain.union(Domain.onlyNull(type));
            }

            return domain;
        }

        return Domain.onlyNull(type);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        Optional<HiveBucketHandle> hiveBucketHandle = getHiveBucketHandle(table);
        if (!hiveBucketHandle.isPresent()) {
            return Optional.empty();
        }
        HiveBucketProperty bucketProperty = table.getStorage().getBucketProperty()
                .orElseThrow(() -> new NoSuchElementException("Bucket property should be set"));
        if (!bucketProperty.getSortedBy().isEmpty() && !isSortedWritingEnabled(session)) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        HivePartitioningHandle partitioningHandle = new HivePartitioningHandle(
                hiveBucketHandle.get().getTableBucketCount(),
                hiveBucketHandle.get().getColumns().stream()
                        .map(HiveColumnHandle::getHiveType)
                        .collect(Collectors.toList()),
                OptionalInt.of(hiveBucketHandle.get().getTableBucketCount()));
        List<String> partitionColumns = hiveBucketHandle.get().getColumns().stream()
                .map(HiveColumnHandle::getName)
                .collect(Collectors.toList());
        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitionColumns));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            return Optional.empty();
        }
        if (!bucketProperty.get().getSortedBy().isEmpty() && !isSortedWritingEnabled(session)) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        Map<String, HiveType> hiveTypeMap = tableMetadata.getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> toHiveType(typeTranslator, column.getType())));
        return Optional.of(new ConnectorNewTableLayout(
                new HivePartitioningHandle(
                        bucketProperty.get().getBucketCount(),
                        bucketedBy.stream()
                                .map(hiveTypeMap::get)
                                .collect(toList()),
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
        return getStatisticsCollectionMetadata(tableMetadata.getColumns(), partitionedBy, false);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = firstNonNull(getPartitionedBy(tableMetadata.getProperties()), ImmutableList.of());
        return getStatisticsCollectionMetadata(tableMetadata.getColumns(), partitionedBy, true);
    }

    private TableStatisticsMetadata getStatisticsCollectionMetadata(List<ColumnMetadata> columns, List<String> partitionedBy, boolean includeRowCount)
    {
        Set<ColumnStatisticMetadata> columnStatistics = columns.stream()
                .filter(column -> !partitionedBy.contains(column.getName()))
                .filter(column -> !column.isHidden())
                .map(this::getColumnStatisticMetadata)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Set<TableStatisticType> tableStatistics = includeRowCount ? ImmutableSet.of(ROW_COUNT) : ImmutableSet.of();
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, partitionedBy);
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(ColumnMetadata columnMetadata)
    {
        return getColumnStatisticMetadata(columnMetadata.getName(), metastore.getSupportedColumnStatistics(columnMetadata.getType()));
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(String columnName, Set<ColumnStatisticType> statisticTypes)
    {
        return statisticTypes.stream()
                .map(type -> new ColumnStatisticMetadata(columnName, type))
                .collect(toImmutableList());
    }

    public void createRole(ConnectorSession session, String role, Optional<PrestoPrincipal> grantor)
    {
        // roles are case insensitive in Hive
        if (RESERVED_ROLES.contains(role)) {
            throw new PrestoException(ALREADY_EXISTS, "Role name cannot be one of the reserved roles: " + RESERVED_ROLES);
        }
        metastore.createRole(role, null);
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        // roles are case insensitive in Hive
        metastore.dropRole(role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return ImmutableSet.copyOf(metastore.listRoles());
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, PrestoPrincipal principal)
    {
        return ImmutableSet.copyOf(metastore.listRoleGrants(principal));
    }

    @Override
    public void grantRoles(ConnectorSession session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor)
    {
        metastore.grantRoles(roles, grantees, withAdminOption, grantor.orElse(new PrestoPrincipal(USER, session.getUser())));
    }

    @Override
    public void revokeRoles(ConnectorSession session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor)
    {
        metastore.revokeRoles(roles, grantees, adminOptionFor, grantor.orElse(new PrestoPrincipal(USER, session.getUser())));
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, PrestoPrincipal principal)
    {
        return ThriftMetastoreUtil.listApplicableRoles(principal, metastore::listRoleGrants)
                .collect(toImmutableSet());
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return ThriftMetastoreUtil.listEnabledRoles(session.getIdentity(), metastore::listRoleGrants)
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

        metastore.grantTablePrivileges(schemaName, tableName, grantee, hivePrivilegeInfos);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Set<HivePrivilegeInfo> hivePrivilegeInfos = privileges.stream()
                .map(privilege -> new HivePrivilegeInfo(toHivePrivilege(privilege), grantOption, new PrestoPrincipal(USER, session.getUser()), new PrestoPrincipal(USER, session.getUser())))
                .collect(toSet());

        metastore.revokeTablePrivileges(schemaName, tableName, grantee, hivePrivilegeInfos);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix schemaTablePrefix)
    {
        Set<PrestoPrincipal> principals = listEnabledPrincipals(metastore, session.getIdentity())
                .collect(toImmutableSet());
        boolean isAdminRoleSet = hasAdminRole(principals);
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        for (SchemaTableName tableName : listTables(session, schemaTablePrefix)) {
            if (isAdminRoleSet) {
                result.addAll(buildGrants(tableName, null));
            }
            else {
                for (PrestoPrincipal grantee : principals) {
                    result.addAll(buildGrants(tableName, grantee));
                }
            }
        }
        return result.build();
    }

    @Override
    public void commitPartition(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;
        stagingFileCommitter.commitFiles(session, handle.getSchemaName(), handle.getTableName(), getPartitionUpdates(fragments));
    }

    @Override
    public void commitPartition(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) tableHandle;
        stagingFileCommitter.commitFiles(session, handle.getSchemaName(), handle.getTableName(), getPartitionUpdates(fragments));
    }

    private List<GrantInfo> buildGrants(SchemaTableName tableName, PrestoPrincipal principal)
    {
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        Set<HivePrivilegeInfo> hivePrivileges = metastore.listTablePrivileges(tableName.getSchemaName(), tableName.getTableName(), principal);
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

    private List<PartitionUpdate> getPartitionUpdates(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toList());
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

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serde)) {
                return format;
            }
        }
        throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serde));
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

    private static List<HiveColumnHandle> getColumnHandles(ConnectorTableMetadata tableMetadata, Set<String> partitionColumnNames, TypeTranslator typeTranslator)
    {
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        return getColumnHandles(tableMetadata.getColumns(), partitionColumnNames, typeTranslator);
    }

    private static List<HiveColumnHandle> getColumnHandles(List<ColumnMetadata> columns, Set<String> partitionColumnNames, TypeTranslator typeTranslator)
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
                    toHiveType(typeTranslator, column.getType()),
                    column.getType().getTypeSignature(),
                    ordinal,
                    columnType,
                    Optional.ofNullable(column.getComment())));
            ordinal++;
        }

        return columnHandles.build();
    }

    private static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table, TypeManager typeManager)
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
        for (Column field : concat(tableColumns, table.getPartitionColumns())) {
            if (field.getComment().isPresent() && !field.getComment().get().equals("from deserializer")) {
                builder.put(field.getName(), field.getComment());
            }
            else {
                builder.put(field.getName(), Optional.empty());
            }
        }
        // add hidden columns
        builder.put(PATH_COLUMN_NAME, Optional.empty());
        if (table.getStorage().getBucketProperty().isPresent()) {
            builder.put(BUCKET_COLUMN_NAME, Optional.empty());
        }

        Map<String, Optional<String>> columnComment = builder.build();

        return handle -> new ColumnMetadata(
                handle.getName(),
                typeManager.getType(handle.getTypeSignature()),
                columnComment.get(handle.getName()).orElse(null),
                columnExtraInfo(handle.isPartitionKey()),
                handle.isHidden());
    }

    @Override
    public void rollback()
    {
        metastore.rollback();
    }

    @Override
    public void commit()
    {
        metastore.commit();
    }

    public static boolean isPartitionsSystemTable(SchemaTableName tableName)
    {
        return tableName.getTableName().endsWith(PARTITIONS_TABLE_SUFFIX) && tableName.getTableName().length() > PARTITIONS_TABLE_SUFFIX.length();
    }

    public static SchemaTableName getSourceTableNameForPartitionsTable(SchemaTableName tableName)
    {
        checkArgument(isPartitionsSystemTable(tableName), "not a partitions table name");
        return new SchemaTableName(
                tableName.getSchemaName(),
                tableName.getTableName().substring(0, tableName.getTableName().length() - PARTITIONS_TABLE_SUFFIX.length()));
    }
}
