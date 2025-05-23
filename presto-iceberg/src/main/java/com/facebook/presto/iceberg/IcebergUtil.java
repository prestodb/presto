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
package com.facebook.presto.iceberg;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.GenericInternalException;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTableVersion.VersionOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.units.DataSize;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.view.View;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_QUERY_ID_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VERSION_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VIEW_COMMENT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.metastore.MetastoreUtil.TABLE_COMMENT;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.FileContent.fromIcebergFileContent;
import static com.facebook.presto.iceberg.IcebergColumnHandle.DATA_SEQUENCE_NUMBER_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergColumnHandle.PATH_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_FORMAT_VERSION;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_PARTITION_VALUE;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_TABLE_TIMESTAMP;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static com.facebook.presto.iceberg.IcebergPartitionType.IDENTITY;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isMergeOnReadModeEnabled;
import static com.facebook.presto.iceberg.IcebergTableProperties.getWriteDataLocation;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static com.google.common.collect.Streams.stream;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Math.toIntExact;
import static java.lang.Math.ulp;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES;
import static org.apache.iceberg.LocationProviders.locationsFor;
import static org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.DELETE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT;
import static org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS;
import static org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_PATH;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_FOLDER_STORAGE_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.FIXED;

public final class IcebergUtil
{
    private static final Logger log = Logger.get(IcebergUtil.class);
    public static final int MIN_FORMAT_VERSION_FOR_DELETE = 2;

    public static final long DOUBLE_POSITIVE_ZERO = 0x0000000000000000L;
    public static final long DOUBLE_POSITIVE_INFINITE = 0x7ff0000000000000L;
    public static final long DOUBLE_NEGATIVE_ZERO = 0x8000000000000000L;
    public static final long DOUBLE_NEGATIVE_INFINITE = 0xfff0000000000000L;

    public static final int REAL_POSITIVE_ZERO = 0x00000000;
    public static final int REAL_POSITIVE_INFINITE = 0x7f800000;
    public static final int REAL_NEGATIVE_ZERO = 0x80000000;
    public static final int REAL_NEGATIVE_INFINITE = 0xff800000;

    protected static final String VIEW_OWNER = "view_owner";

    private IcebergUtil() {}

    public static boolean isIcebergTable(com.facebook.presto.hive.metastore.Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    public static Table getIcebergTable(ConnectorMetadata metadata, ConnectorSession session, SchemaTableName table)
    {
        checkArgument(metadata instanceof IcebergAbstractMetadata, "metadata must be instance of IcebergAbstractMetadata!");
        IcebergAbstractMetadata icebergMetadata = (IcebergAbstractMetadata) metadata;
        return icebergMetadata.getIcebergTable(session, table);
    }

    public static Table getShallowWrappedIcebergTable(Schema schema, PartitionSpec spec, Map<String, String> properties, Optional<SortOrder> sortOrder)
    {
        return new PrestoIcebergTableForMetricsConfig(schema, spec, properties, sortOrder);
    }

    public static Table getHiveIcebergTable(ExtendedHiveMetastore metastore, HdfsEnvironment hdfsEnvironment, IcebergHiveTableOperationsConfig config, ManifestFileCache manifestFileCache, ConnectorSession session, IcebergCatalogName catalogName, SchemaTableName table)
    {
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        TableOperations operations = new HiveTableOperations(
                metastore,
                new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getClientTags(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, session.getWarningCollector(), session.getRuntimeStats()),
                hdfsEnvironment,
                hdfsContext,
                config,
                manifestFileCache,
                table.getSchemaName(),
                table.getTableName());
        return new BaseTable(operations, fullTableName(catalogName.getCatalogName(), TableIdentifier.of(table.getSchemaName(), table.getTableName())));
    }

    public static Table getNativeIcebergTable(IcebergNativeCatalogFactory catalogFactory, ConnectorSession session, SchemaTableName table)
    {
        return catalogFactory.getCatalog(session).loadTable(toIcebergTableIdentifier(table, catalogFactory.isNestedNamespaceEnabled()));
    }

    public static View getNativeIcebergView(IcebergNativeCatalogFactory catalogFactory, ConnectorSession session, SchemaTableName table)
    {
        Catalog catalog = catalogFactory.getCatalog(session);
        if (!(catalog instanceof ViewCatalog)) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support get views");
        }
        return ((ViewCatalog) catalog).loadView(toIcebergTableIdentifier(table, catalogFactory.isNestedNamespaceEnabled()));
    }

    public static List<IcebergColumnHandle> getPartitionKeyColumnHandles(IcebergTableHandle tableHandle, Table table, TypeManager typeManager)
    {
        Set<PartitionSpec> partitionSpecs = tableHandle.getIcebergTableName().getSnapshotId()
                .map(snapshot -> table.snapshot(snapshot).allManifests(table.io()).stream()
                        .map(ManifestFile::partitionSpecId)
                        .map(specId -> table.specs().get(specId))
                        .collect(toImmutableSet()))
                .orElseGet(() -> ImmutableSet.copyOf(table.specs().values()));   // No snapshot, so no data. This case doesn't matter.

        return table.spec().fields().stream()
                .filter(field -> field.transform().isIdentity() &&
                        partitionSpecs.stream()
                                .allMatch(partitionSpec -> partitionSpec.getFieldsBySourceId(field.sourceId()).stream()
                                        .anyMatch(partitionField -> partitionField.transform().isIdentity())))
                .map(field -> IcebergColumnHandle.create(table.schema().findField(field.sourceId()), typeManager, PARTITION_KEY))
                .collect(toImmutableList());
    }

    public static Optional<Long> resolveSnapshotIdByName(Table table, IcebergTableName name)
    {
        if (name.getSnapshotId().isPresent()) {
            if (table.snapshot(name.getSnapshotId().get()) == null) {
                throw new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, format("Invalid snapshot [%s] for table: %s", name.getSnapshotId().get(), table));
            }
            return name.getSnapshotId();
        }

        if (name.getTableType() == IcebergTableType.CHANGELOG) {
            return Optional.ofNullable(SnapshotUtil.oldestAncestor(table)).map(Snapshot::snapshotId);
        }

        return tryGetCurrentSnapshot(table).map(Snapshot::snapshotId);
    }

    public static long getSnapshotIdTimeOperator(Table table, long millisUtc, VersionOperator operator)
    {
        return table.history().stream()
                .filter(logEntry -> operator == VersionOperator.EQUAL ? logEntry.timestampMillis() <= millisUtc : logEntry.timestampMillis() < millisUtc)
                .max(comparing(HistoryEntry::timestampMillis))
                .orElseThrow(() -> new PrestoException(ICEBERG_INVALID_TABLE_TIMESTAMP, format("No history found based on timestamp for table %s", table.name())))
                .snapshotId();
    }

    public static Map<String, List<String>> getPartitionFields(PartitionSpec partitionSpec, IcebergPartitionType partitionType)
    {
        Map<String, List<String>> partitionFields = new HashMap<>();

        switch (partitionType) {
            case IDENTITY:
                for (int i = 0; i < partitionSpec.fields().size(); i++) {
                    PartitionField field = partitionSpec.fields().get(i);
                    if (field.transform().isIdentity()) {
                        partitionFields.put(field.name(), ImmutableList.of(field.transform().toString()));
                    }
                }
                break;
            case ALL:
                for (int i = 0; i < partitionSpec.fields().size(); i++) {
                    PartitionField field = partitionSpec.fields().get(i);
                    String sourceColumnName = partitionSpec.schema().findColumnName(field.sourceId());
                    partitionFields.computeIfAbsent(sourceColumnName, k -> new ArrayList<>())
                            .add(field.transform().toString());
                }
                break;
        }

        return partitionFields;
    }

    public static List<IcebergColumnHandle> getColumns(Schema schema, PartitionSpec partitionSpec, TypeManager typeManager)
    {
        return getColumns(schema.columns().stream().map(NestedField::fieldId), schema, partitionSpec, typeManager);
    }

    public static List<IcebergColumnHandle> getColumns(Stream<Integer> fields, Schema schema, PartitionSpec partitionSpec, TypeManager typeManager)
    {
        Set<String> partitionFieldNames = getPartitionFields(partitionSpec, IDENTITY).keySet();

        return fields
                .map(schema::findField)
                .map(column -> partitionFieldNames.contains(column.name()) ?
                        IcebergColumnHandle.create(column, typeManager, PARTITION_KEY) :
                        IcebergColumnHandle.create(column, typeManager, REGULAR))
                .collect(toImmutableList());
    }

    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().isIdentity()) {
                columns.put(field, i);
            }
        }
        return columns.build();
    }

    public static Set<Integer> getPartitionSpecsIncludingValidData(Table icebergTable, Optional<Long> snapshotId)
    {
        return snapshotId.map(snapshot -> icebergTable.snapshot(snapshot).allManifests(icebergTable.io()).stream()
                        .filter(manifestFile -> manifestFile.hasAddedFiles() || manifestFile.hasExistingFiles())
                        .map(ManifestFile::partitionSpecId)
                        .collect(toImmutableSet()))
                .orElseGet(() -> ImmutableSet.copyOf(icebergTable.specs().keySet()));   // No snapshot, so no data. This case doesn't matter.
    }

    public static List<Column> toHiveColumns(List<NestedField> columns)
    {
        return columns.stream()
                .map(column -> new Column(
                        column.name(),
                        HiveType.toHiveType(HiveSchemaUtil.convert(column.type())),
                        Optional.empty(),
                        Optional.empty()))
                .collect(toImmutableList());
    }

    public static FileFormat getFileFormat(Table table)
    {
        return FileFormat.valueOf(table.properties()
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }

    public static Optional<String> getTableComment(Table table)
    {
        return Optional.ofNullable(table.properties().get(TABLE_COMMENT));
    }

    public static Optional<String> getViewComment(View view)
    {
        return Optional.ofNullable(view.properties().get(TABLE_COMMENT));
    }

    public static TableScan getTableScan(TupleDomain<IcebergColumnHandle> predicates, Optional<Long> snapshotId, Table icebergTable)
    {
        Expression expression = ExpressionConverter.toIcebergExpression(predicates);
        TableScan tableScan = icebergTable.newScan().filter(expression);
        return snapshotId
                .map(id -> isSnapshot(icebergTable, id) ? tableScan.useSnapshot(id) : tableScan.asOfTime(id))
                .orElse(tableScan);
    }

    private static boolean isSnapshot(Table icebergTable, Long id)
    {
        return stream(icebergTable.snapshots())
                .anyMatch(snapshot -> snapshot.snapshotId() == id);
    }

    public static LocationProvider getLocationProvider(SchemaTableName schemaTableName, String tableLocation, Map<String, String> storageProperties)
    {
        if (storageProperties.containsKey(WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new PrestoException(NOT_SUPPORTED, "Table " + schemaTableName + " specifies " + storageProperties.get(WRITE_LOCATION_PROVIDER_IMPL) +
                    " as a location provider. Writing to Iceberg tables with custom location provider is not supported.");
        }
        return locationsFor(tableLocation, storageProperties);
    }

    public static TableScan buildTableScan(Table icebergTable, MetadataTableType metadataTableType)
    {
        return createMetadataTableInstance(icebergTable, metadataTableType).newScan();
    }

    public static Map<String, Integer> columnNameToPositionInSchema(Schema schema)
    {
        return mapWithIndex(schema.columns().stream(),
                (column, position) -> immutableEntry(column.name(), toIntExact(position)))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    public static void validateTableMode(ConnectorSession session, org.apache.iceberg.Table table)
    {
        if (isMergeOnReadModeEnabled(session)) {
            return;
        }

        String deleteMode = table.properties().get(DELETE_MODE);
        String mergeMode = table.properties().get(MERGE_MODE);
        String updateMode = table.properties().get(UPDATE_MODE);
        if (Stream.of(deleteMode, mergeMode, updateMode).anyMatch(s -> Objects.equals(s, RowLevelOperationMode.MERGE_ON_READ.modeName()))) {
            throw new PrestoException(NOT_SUPPORTED, "merge-on-read table mode not supported yet");
        }
    }

    public static Map<String, String> createIcebergViewProperties(ConnectorSession session, String prestoVersion)
    {
        return ImmutableMap.<String, String>builder()
                .put(TABLE_COMMENT, PRESTO_VIEW_COMMENT)
                .put(PRESTO_VIEW_FLAG, "true")
                .put(PRESTO_VERSION_NAME, prestoVersion)
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE)
                .put(VIEW_OWNER, session.getUser())
                .build();
    }

    public static Optional<Map<String, String>> tryGetProperties(Table table)
    {
        try {
            return Optional.ofNullable(table.properties());
        }
        catch (TableNotFoundException e) {
            log.warn(String.format("Unable to fetch properties for table %s: %s", table.name(), e.getMessage()));
            return Optional.empty();
        }
    }

    public static Optional<Snapshot> tryGetCurrentSnapshot(Table table)
    {
        try {
            return Optional.ofNullable(table.currentSnapshot());
        }
        catch (TableNotFoundException e) {
            log.warn(String.format("Unable to fetch snapshot for table %s: %s", table.name(), e.getMessage()));
            return Optional.empty();
        }
    }

    public static Optional<String> tryGetLocation(Table table)
    {
        try {
            return Optional.ofNullable(table.location());
        }
        catch (TableNotFoundException e) {
            log.warn(String.format("Unable to fetch location for table %s: %s", table.name(), e.getMessage()));
            return Optional.empty();
        }
    }

    public static List<SortField> getSortFields(Table table)
    {
        try {
            return table.sortOrder().fields().stream()
                    .filter(field -> field.transform().isIdentity())
                    .map(SortField::fromIceberg)
                    .collect(toImmutableList());
        }
        catch (Exception e) {
            log.warn(String.format("Unable to fetch sort fields for table %s: %s", table.name(), e.getMessage()));
            return ImmutableList.of();
        }
    }

    private static boolean isValidPartitionType(Type type)
    {
        return type instanceof DecimalType ||
                BOOLEAN.equals(type) ||
                TINYINT.equals(type) ||
                SMALLINT.equals(type) ||
                INTEGER.equals(type) ||
                BIGINT.equals(type) ||
                REAL.equals(type) ||
                DOUBLE.equals(type) ||
                DATE.equals(type) ||
                type instanceof TimestampType ||
                TIME.equals(type) ||
                VARBINARY.equals(type) ||
                isVarcharType(type) ||
                isCharType(type);
    }

    private static void verifyPartitionTypeSupported(String partitionName, Type type)
    {
        if (!isValidPartitionType(type)) {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported type [%s] for partition: %s", type, partitionName));
        }
    }

    private static NullableValue parsePartitionValue(
            FileFormat fileFormat,
            String partitionStringValue,
            Type prestoType,
            String partitionName)
    {
        verifyPartitionTypeSupported(partitionName, prestoType);

        Object partitionValue = deserializePartitionValue(prestoType, partitionStringValue, partitionName);
        return partitionValue == null ? NullableValue.asNull(prestoType) : NullableValue.of(prestoType, partitionValue);
    }

    // Strip the constraints on metadata columns like "$path", "$data_sequence_number" from the list.
    public static <U> TupleDomain<IcebergColumnHandle> getNonMetadataColumnConstraints(TupleDomain<U> allConstraints)
    {
        return allConstraints.transform(c -> isMetadataColumnId(((IcebergColumnHandle) c).getId()) ? null : (IcebergColumnHandle) c);
    }

    public static <U> TupleDomain<IcebergColumnHandle> getMetadataColumnConstraints(TupleDomain<U> allConstraints)
    {
        return allConstraints.transform(c -> isMetadataColumnId(((IcebergColumnHandle) c).getId()) ? (IcebergColumnHandle) c : null);
    }

    public static boolean metadataColumnsMatchPredicates(TupleDomain<IcebergColumnHandle> constraints, String path, long dataSequenceNumber)
    {
        if (constraints.isAll()) {
            return true;
        }

        boolean matches = true;
        if (constraints.getDomains().isPresent()) {
            for (Map.Entry<IcebergColumnHandle, Domain> constraint : constraints.getDomains().get().entrySet()) {
                if (constraint.getKey() == PATH_COLUMN_HANDLE) {
                    matches &= constraint.getValue().includesNullableValue(utf8Slice(path));
                }
                else if (constraint.getKey() == DATA_SEQUENCE_NUMBER_COLUMN_HANDLE) {
                    matches &= constraint.getValue().includesNullableValue(dataSequenceNumber);
                }
            }
        }

        return matches;
    }

    public static List<HivePartition> getPartitions(
            TypeManager typeManager,
            ConnectorTableHandle tableHandle,
            Table icebergTable,
            Constraint<ColumnHandle> constraint,
            List<IcebergColumnHandle> partitionColumns)
    {
        IcebergTableName name = ((IcebergTableHandle) tableHandle).getIcebergTableName();
        FileFormat fileFormat = getFileFormat(icebergTable);
        // Empty iceberg table would cause `snapshotId` not present
        Optional<Long> snapshotId = resolveSnapshotIdByName(icebergTable, name);
        if (!snapshotId.isPresent()) {
            return ImmutableList.of();
        }

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(getNonMetadataColumnConstraints(constraint
                        .getSummary()
                        .simplify())))
                .useSnapshot(snapshotId.get());

        Set<HivePartition> partitions = new HashSet<>();

        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                // If exists delete files, skip the metadata optimization based on partition values as they might become incorrect
                if (!fileScanTask.deletes().isEmpty()) {
                    return ImmutableList.of(new HivePartition(((IcebergTableHandle) tableHandle).getSchemaTableName()));
                }
                StructLike partition = fileScanTask.file().partition();
                PartitionSpec spec = fileScanTask.spec();
                Map<PartitionField, Integer> fieldToIndex = getIdentityPartitions(spec);
                ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();

                fieldToIndex.forEach((field, index) -> {
                    int id = field.sourceId();
                    org.apache.iceberg.types.Type type = spec.schema().findType(id);
                    Class<?> javaClass = type.typeId().javaClass();
                    Object value = partition.get(index, javaClass);
                    String partitionStringValue;

                    if (value == null) {
                        partitionStringValue = null;
                    }
                    else {
                        if (type.typeId() == FIXED || type.typeId() == BINARY) {
                            partitionStringValue = Base64.getEncoder().encodeToString(((ByteBuffer) value).array());
                        }
                        else {
                            partitionStringValue = value.toString();
                        }
                    }

                    NullableValue partitionValue = parsePartitionValue(fileFormat, partitionStringValue, toPrestoType(type, typeManager), partition.toString());
                    Optional<IcebergColumnHandle> column = partitionColumns.stream()
                            .filter(icebergColumnHandle -> Objects.equals(icebergColumnHandle.getId(), field.sourceId()))
                            .findAny();

                    if (column.isPresent()) {
                        builder.put(column.get(), partitionValue);
                    }
                });

                Map<ColumnHandle, NullableValue> values = builder.build();
                HivePartition newPartition = new HivePartition(
                        ((IcebergTableHandle) tableHandle).getSchemaTableName(),
                        new PartitionNameWithVersion(partition.toString(), Optional.empty()),
                        values);

                boolean isIncludePartition = true;
                Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().get();
                for (IcebergColumnHandle column : partitionColumns) {
                    NullableValue value = newPartition.getKeys().get(column);
                    Domain allowedDomain = domains.get(column);
                    if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                        isIncludePartition = false;
                        break;
                    }
                }

                if (constraint.predicate().isPresent() && !constraint.predicate().get().test(newPartition.getKeys())) {
                    isIncludePartition = false;
                }

                if (isIncludePartition) {
                    partitions.add(newPartition);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new ArrayList<>(partitions);
    }

    public static Optional<Schema> tryGetSchema(Table table)
    {
        try {
            return Optional.ofNullable(table.schema());
        }
        catch (TableNotFoundException e) {
            log.warn(String.format("Unable to fetch schema for table %s: %s", table.name(), e.getMessage()));
            return Optional.empty();
        }
    }

    public static Schema schemaFromHandles(List<IcebergColumnHandle> columns)
    {
        List<NestedField> icebergColumns = columns.stream()
                .map(column -> NestedField.optional(column.getId(), column.getName(), toIcebergType(column.getType())))
                .collect(toImmutableList());
        return new Schema(Types.StructType.of(icebergColumns).asStructType().fields());
    }

    public static Object deserializePartitionValue(Type type, String valueString, String name)
    {
        if (valueString == null) {
            return null;
        }

        try {
            if (type.equals(BOOLEAN)) {
                if (valueString.equalsIgnoreCase("true")) {
                    return true;
                }
                if (valueString.equalsIgnoreCase("false")) {
                    return false;
                }
                throw new IllegalArgumentException();
            }
            if (type.equals(INTEGER)) {
                return parseLong(valueString);
            }
            if (type.equals(BIGINT)) {
                return parseLong(valueString);
            }
            if (type.equals(REAL)) {
                return (long) floatToRawIntBits(parseFloat(valueString));
            }
            if (type.equals(DOUBLE)) {
                return parseDouble(valueString);
            }
            if (type.equals(TIMESTAMP) || type.equals(TIME)) {
                return MICROSECONDS.toMillis(parseLong(valueString));
            }
            if (type.equals(DATE) || type.equals(TIMESTAMP_MICROSECONDS)) {
                return parseLong(valueString);
            }
            if (type instanceof VarcharType) {
                return utf8Slice(valueString);
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                return wrappedBuffer(Base64.getDecoder().decode(valueString));
            }
            if (isShortDecimal(type) || isLongDecimal(type)) {
                DecimalType decimalType = (DecimalType) type;
                BigDecimal decimal = new BigDecimal(valueString);
                decimal = decimal.setScale(decimalType.getScale(), BigDecimal.ROUND_UNNECESSARY);
                checkArgument(decimal.precision() <= decimalType.getPrecision());
                BigInteger unscaledValue = decimal.unscaledValue();
                return isShortDecimal(type) ? unscaledValue.longValue() : Decimals.encodeUnscaledValue(unscaledValue);
            }
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(ICEBERG_INVALID_PARTITION_VALUE, format(
                    "Invalid partition value '%s' for %s partition key: %s",
                    valueString,
                    type.getDisplayName(),
                    name));
        }
        // Iceberg tables don't partition by non-primitive-type columns.
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid partition type " + type.toString());
    }

    /**
     * Returns the adjacent value that compares bigger than or less than {@code value} based on parameter {@code isPrevious}.
     * <p>
     * The type of the value must match {@code #type.getJavaType()}.
     *
     * @throws IllegalStateException if the type is not {@code #isOrderable()}
     */
    public static Optional<Object> getAdjacentValue(Type type, Object value, boolean isPrevious)
    {
        if (!type.isOrderable()) {
            throw new IllegalStateException("Type is not orderable: " + type);
        }
        requireNonNull(value, "value is null");

        if (type.equals(BIGINT) || type instanceof TimestampType) {
            return getBigintAdjacentValue(value, isPrevious);
        }

        if (type.equals(INTEGER) || type.equals(DATE)) {
            return getIntegerAdjacentValue(value, isPrevious);
        }

        if (type.equals(SMALLINT)) {
            return getSmallIntAdjacentValue(value, isPrevious);
        }

        if (type.equals(TINYINT)) {
            return getTinyIntAdjacentValue(value, isPrevious);
        }

        if (type.equals(DOUBLE)) {
            return getDoubleAdjacentValue(value, isPrevious);
        }

        if (type.equals(REAL)) {
            return getRealAdjacentValue(value, isPrevious);
        }

        return Optional.empty();
    }

    public static Map<Integer, HivePartitionKey> getPartitionKeys(ContentScanTask<DataFile> scanTask)
    {
        StructLike partition = scanTask.file().partition();
        PartitionSpec spec = scanTask.spec();
        return getPartitionKeys(spec, partition);
    }

    public static Map<Integer, HivePartitionKey> getPartitionKeys(PartitionSpec spec, StructLike partition)
    {
        Map<Integer, HivePartitionKey> partitionKeys = new HashMap<>();

        int index = 0;
        for (PartitionField field : spec.fields()) {
            int sourceId = field.sourceId();
            String colName = field.name();
            org.apache.iceberg.types.Type sourceType = spec.schema().findType(sourceId);
            org.apache.iceberg.types.Type type = field.transform().getResultType(sourceType);
            Class<?> javaClass = type.typeId().javaClass();
            Object value = partition.get(index, javaClass);

            if (value == null) {
                partitionKeys.put(field.fieldId(), new HivePartitionKey(colName, Optional.empty()));
            }
            else {
                HivePartitionKey partitionValue;
                if (type.typeId() == FIXED || type.typeId() == BINARY) {
                    // this is safe because Iceberg PartitionData directly wraps the byte array
                    partitionValue = new HivePartitionKey(colName, Optional.of(Base64.getEncoder().encodeToString(((ByteBuffer) value).array())));
                }
                else {
                    partitionValue = new HivePartitionKey(colName, Optional.of(value.toString()));
                }
                partitionKeys.put(field.fieldId(), partitionValue);
                if (field.transform().isIdentity()) {
                    partitionKeys.put(sourceId, partitionValue);
                }
            }
            index += 1;
        }

        return Collections.unmodifiableMap(partitionKeys);
    }

    public static Map<String, String> loadCachingProperties(IcebergConfig icebergConfig)
    {
        return ImmutableMap.<String, String>builderWithExpectedSize(4)
                .put(IO_MANIFEST_CACHE_ENABLED, "true")
                .put(IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, String.valueOf(icebergConfig.getMaxManifestCacheSize()))
                .put(IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH, String.valueOf(icebergConfig.getManifestCacheMaxContentLength()))
                .put(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS, String.valueOf(icebergConfig.getManifestCacheExpireDuration()))
                .build();
    }

    public static long getDataSequenceNumber(ContentFile<?> file)
    {
        if (file.dataSequenceNumber() != null) {
            return file.dataSequenceNumber();
        }
        return file.fileSequenceNumber();
    }

    /**
     * Provides the delete files that need to be applied to the given table snapshot.
     *
     * @param table The table to provide deletes for
     * @param snapshot The snapshot id to use
     * @param filter Filters to apply during planning
     * @param requestedPartitionSpec If provided, only delete files for this partition spec will be provided
     * @param requestedSchema If provided, only delete files with this schema will be provided
     */
    public static CloseableIterable<DeleteFile> getDeleteFiles(Table table,
            long snapshot,
            TupleDomain<IcebergColumnHandle> filter,
            Optional<Set<Integer>> requestedPartitionSpec,
            Optional<Set<Integer>> requestedSchema)
    {
        Expression filterExpression = toIcebergExpression(filter);
        CloseableIterable<FileScanTask> fileTasks = table.newScan().useSnapshot(snapshot).filter(filterExpression).planFiles();

        return new CloseableIterable<DeleteFile>()
        {
            @Override
            public void close()
                    throws IOException
            {
                fileTasks.close();
            }

            @Override
            public CloseableIterator<DeleteFile> iterator()
            {
                return new DeleteFilesIterator(table.specs(), fileTasks.iterator(), requestedPartitionSpec, requestedSchema);
            }
        };
    }

    private static Optional<Object> getBigintAdjacentValue(Object value, boolean isPrevious)
    {
        long currentValue = (long) value;
        if (isPrevious) {
            if (currentValue == Long.MIN_VALUE) {
                return Optional.empty();
            }
            return Optional.of(currentValue - 1);
        }
        else {
            if (currentValue == Long.MAX_VALUE) {
                return Optional.empty();
            }
            return Optional.of(currentValue + 1);
        }
    }

    private static Optional<Object> getIntegerAdjacentValue(Object value, boolean isPrevious)
    {
        long currentValue = toIntExact((long) value);
        if (isPrevious) {
            if (currentValue == Integer.MIN_VALUE) {
                return Optional.empty();
            }
            return Optional.of(currentValue - 1);
        }
        else {
            if (currentValue == Integer.MAX_VALUE) {
                return Optional.empty();
            }
            return Optional.of(currentValue + 1);
        }
    }

    private static Optional<Object> getSmallIntAdjacentValue(Object value, boolean isPrevious)
    {
        long currentValue = (long) value;
        if (currentValue > Short.MAX_VALUE) {
            throw new GenericInternalException(format("Value %d exceeds MAX_SHORT", value));
        }
        if (currentValue < Short.MIN_VALUE) {
            throw new GenericInternalException(format("Value %d is less than MIN_SHORT", value));
        }

        if (isPrevious) {
            if (currentValue == Short.MIN_VALUE) {
                return Optional.empty();
            }
            return Optional.of(currentValue - 1);
        }
        else {
            if (currentValue == Short.MAX_VALUE) {
                return Optional.empty();
            }
            return Optional.of(currentValue + 1);
        }
    }

    private static Optional<Object> getTinyIntAdjacentValue(Object value, boolean isPrevious)
    {
        long currentValue = (long) value;
        if (currentValue > Byte.MAX_VALUE) {
            throw new GenericInternalException(format("Value %d exceeds MAX_BYTE", value));
        }
        if (currentValue < Byte.MIN_VALUE) {
            throw new GenericInternalException(format("Value %d is less than MIN_BYTE", value));
        }

        if (isPrevious) {
            if (currentValue == Byte.MIN_VALUE) {
                return Optional.empty();
            }
            return Optional.of(currentValue - 1);
        }
        else {
            if (currentValue == Byte.MAX_VALUE) {
                return Optional.empty();
            }
            return Optional.of(currentValue + 1);
        }
    }

    private static Optional<Object> getDoubleAdjacentValue(Object value, boolean isPrevious)
    {
        long longBitForDouble = (long) value;
        if (longBitForDouble > DOUBLE_POSITIVE_INFINITE && longBitForDouble < DOUBLE_NEGATIVE_ZERO ||
                longBitForDouble > DOUBLE_NEGATIVE_INFINITE && longBitForDouble < DOUBLE_POSITIVE_ZERO) {
            throw new GenericInternalException(format("Value %d exceeds the range of double", longBitForDouble));
        }

        if (isPrevious) {
            if (longBitForDouble == DOUBLE_NEGATIVE_INFINITE) {
                return Optional.empty();
            }
            if (longBitForDouble == DOUBLE_POSITIVE_INFINITE) {
                return Optional.of(DOUBLE_POSITIVE_INFINITE - 1);
            }
            double currentValue = longBitsToDouble(longBitForDouble);
            return Optional.of(doubleToRawLongBits(currentValue - ulp(currentValue)));
        }
        else {
            if (longBitForDouble == DOUBLE_POSITIVE_INFINITE) {
                return Optional.empty();
            }
            if (longBitForDouble == DOUBLE_NEGATIVE_INFINITE) {
                return Optional.of(DOUBLE_NEGATIVE_INFINITE - 1);
            }
            double currentValue = longBitsToDouble(longBitForDouble);
            return Optional.of(doubleToRawLongBits(currentValue + ulp(currentValue)));
        }
    }

    private static Optional<Object> getRealAdjacentValue(Object value, boolean isPrevious)
    {
        int intBitForFloat = (int) value;
        if (intBitForFloat > REAL_POSITIVE_INFINITE && intBitForFloat < REAL_NEGATIVE_ZERO ||
                intBitForFloat > REAL_NEGATIVE_INFINITE && intBitForFloat < REAL_POSITIVE_ZERO) {
            throw new GenericInternalException(format("Value %d exceeds the range of real", intBitForFloat));
        }

        if (isPrevious) {
            if (intBitForFloat == REAL_NEGATIVE_INFINITE) {
                return Optional.empty();
            }
            if (intBitForFloat == REAL_POSITIVE_INFINITE) {
                return Optional.of(REAL_POSITIVE_INFINITE - 1);
            }
            float currentValue = intBitsToFloat(intBitForFloat);
            return Optional.of(floatToRawIntBits(currentValue - ulp(currentValue)));
        }
        else {
            if (intBitForFloat == REAL_POSITIVE_INFINITE) {
                return Optional.empty();
            }
            if (intBitForFloat == REAL_NEGATIVE_INFINITE) {
                return Optional.of(REAL_NEGATIVE_INFINITE - 1);
            }
            float currentValue = intBitsToFloat(intBitForFloat);
            return Optional.of(floatToRawIntBits(currentValue + ulp(currentValue)));
        }
    }

    private static class DeleteFilesIterator
            implements CloseableIterator<DeleteFile>
    {
        private final Set<String> seenFiles = new HashSet<>();
        private final Map<Integer, PartitionSpec> partitionSpecsById;
        private CloseableIterator<FileScanTask> fileTasks;
        private final Optional<Set<Integer>> requestedPartitionSpec;
        private final Optional<Set<Integer>> requestedSchema;
        private Iterator<DeleteFile> currentDeletes = emptyIterator();
        private DeleteFile currentFile;

        private DeleteFilesIterator(Map<Integer, PartitionSpec> partitionSpecsById,
                CloseableIterator<FileScanTask> fileTasks,
                Optional<Set<Integer>> requestedPartitionSpec,
                Optional<Set<Integer>> requestedSchema)
        {
            this.partitionSpecsById = partitionSpecsById;
            this.fileTasks = fileTasks;
            this.requestedPartitionSpec = requestedPartitionSpec;
            this.requestedSchema = requestedSchema;
        }

        @Override
        public boolean hasNext()
        {
            return currentFile != null || advance();
        }

        private boolean advance()
        {
            currentFile = null;
            while (currentFile == null && (currentDeletes.hasNext() || fileTasks.hasNext())) {
                if (!currentDeletes.hasNext()) {
                    currentDeletes = fileTasks.next().deletes().iterator();
                }
                while (currentDeletes.hasNext()) {
                    DeleteFile deleteFile = currentDeletes.next();
                    if (shouldIncludeFile(deleteFile, partitionSpecsById.get(deleteFile.specId()))) {
                        // If there is a requested schema only include files that match it
                        if (seenFiles.add(deleteFile.path().toString())) {
                            currentFile = deleteFile;
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public DeleteFile next()
        {
            DeleteFile result = currentFile;
            advance();
            return result;
        }

        private boolean shouldIncludeFile(DeleteFile file, PartitionSpec partitionSpec)
        {
            boolean matchesPartition = !requestedPartitionSpec.isPresent() ||
                    requestedPartitionSpec.get().equals(partitionSpec.fields().stream().map(PartitionField::fieldId).collect(Collectors.toSet()));
            return matchesPartition &&
                    (fromIcebergFileContent(file.content()) == POSITION_DELETES ||
                            equalityFieldIdsFulfillRequestSchema(file, partitionSpec));
        }

        private boolean equalityFieldIdsFulfillRequestSchema(DeleteFile file, PartitionSpec partitionSpec)
        {
            Set<Integer> identityPartitionSourceIds = partitionSpec.fields().stream()
                    .filter(partitionField -> partitionField.transform().isIdentity())
                    .map(PartitionField::sourceId).collect(Collectors.toSet());

            // Column ids in `requestedSchema` do not include identity partition columns for the sake of `delete-schema-merging` within the same partition spec.
            // So we need to filter out the identity partition columns from delete files' `equalityFiledIds` when determine if they fulfill the `requestedSchema`.
            return !requestedSchema.isPresent() ||
                    requestedSchema.get().equals(Sets.difference(ImmutableSet.copyOf(file.equalityFieldIds()), identityPartitionSourceIds));
        }

        @Override
        public void close()
                throws IOException
        {
            fileTasks.close();
            // TODO: remove this after org.apache.iceberg.io.CloseableIterator'withClose
            // correct release resources holds by iterator.
            // (and make it final)
            fileTasks = CloseableIterator.empty();
        }
    }

    public static Map<String, String> populateTableProperties(IcebergAbstractMetadata metadata, ConnectorTableMetadata tableMetadata, IcebergTableProperties tableProperties, FileFormat fileFormat, ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builderWithExpectedSize(5);

        String writeDataLocation = getWriteDataLocation(tableMetadata.getProperties());
        if (!isNullOrEmpty(writeDataLocation)) {
            propertiesBuilder.put(WRITE_DATA_LOCATION, writeDataLocation);
        }
        else {
            Optional<String> dataLocation = metadata.getDataLocationBasedOnWarehouseDataDir(tableMetadata.getTable());
            dataLocation.ifPresent(location -> propertiesBuilder.put(WRITE_DATA_LOCATION, location));
        }

        Integer commitRetries = tableProperties.getCommitRetries(session, tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toString());
        propertiesBuilder.put(COMMIT_NUM_RETRIES, String.valueOf(commitRetries));
        switch (fileFormat) {
            case PARQUET:
                propertiesBuilder.put(PARQUET_COMPRESSION, getCompressionCodec(session).getParquetCompressionCodec().get().toString());
                break;
            case ORC:
                propertiesBuilder.put(ORC_COMPRESSION, getCompressionCodec(session).getOrcCompressionKind().name());
                break;
        }
        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }

        String formatVersion = tableProperties.getFormatVersion(session, tableMetadata.getProperties());
        verify(formatVersion != null, "Format version cannot be null");
        propertiesBuilder.put(FORMAT_VERSION, formatVersion);

        if (parseFormatVersion(formatVersion) < MIN_FORMAT_VERSION_FOR_DELETE) {
            propertiesBuilder.put(DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
            propertiesBuilder.put(UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
        }
        else {
            RowLevelOperationMode deleteMode = tableProperties.getDeleteMode(session, tableMetadata.getProperties());
            propertiesBuilder.put(DELETE_MODE, deleteMode.modeName());
            RowLevelOperationMode updateMode = tableProperties.getUpdateMode(tableMetadata.getProperties());
            propertiesBuilder.put(UPDATE_MODE, updateMode.modeName());
        }

        Integer metadataPreviousVersionsMax = tableProperties.getMetadataPreviousVersionsMax(session, tableMetadata.getProperties());
        propertiesBuilder.put(METADATA_PREVIOUS_VERSIONS_MAX, String.valueOf(metadataPreviousVersionsMax));

        Boolean metadataDeleteAfterCommit = tableProperties.isMetadataDeleteAfterCommit(session, tableMetadata.getProperties());
        propertiesBuilder.put(METADATA_DELETE_AFTER_COMMIT_ENABLED, String.valueOf(metadataDeleteAfterCommit));

        Integer metricsMaxInferredColumn = tableProperties.getMetricsMaxInferredColumn(session, tableMetadata.getProperties());
        propertiesBuilder.put(METRICS_MAX_INFERRED_COLUMN_DEFAULTS, String.valueOf(metricsMaxInferredColumn));

        propertiesBuilder.put(SPLIT_SIZE, String.valueOf(IcebergTableProperties.getTargetSplitSize(tableMetadata.getProperties())));

        return propertiesBuilder.build();
    }

    public static int parseFormatVersion(String formatVersion)
    {
        try {
            return parseInt(formatVersion);
        }
        catch (NumberFormatException | IndexOutOfBoundsException e) {
            throw new PrestoException(ICEBERG_INVALID_FORMAT_VERSION, "Unable to parse user provided format version");
        }
    }

    public static RowLevelOperationMode getDeleteMode(Table table)
    {
        return RowLevelOperationMode.fromName(table.properties()
                .getOrDefault(DELETE_MODE, DELETE_MODE_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }

    public static RowLevelOperationMode getUpdateMode(Table table)
    {
        return RowLevelOperationMode.fromName(table.properties()
                .getOrDefault(UPDATE_MODE, UPDATE_MODE_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }

    public static int getMetadataPreviousVersionsMax(Table table)
    {
        return Integer.parseInt(table.properties()
                .getOrDefault(METADATA_PREVIOUS_VERSIONS_MAX,
                        String.valueOf(METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT)));
    }

    public static boolean isMetadataDeleteAfterCommit(Table table)
    {
        return Boolean.valueOf(table.properties()
                .getOrDefault(METADATA_DELETE_AFTER_COMMIT_ENABLED,
                        String.valueOf(METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT)));
    }

    public static int getMetricsMaxInferredColumn(Table table)
    {
        return Integer.parseInt(table.properties()
                .getOrDefault(METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
                        String.valueOf(METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT)));
    }

    public static Optional<PartitionData> partitionDataFromJson(PartitionSpec spec, Optional<String> partitionDataAsJson)
    {
        org.apache.iceberg.types.Type[] partitionColumnTypes = spec.fields().stream()
                .map(field -> field.transform().getResultType(
                        spec.schema().findType(field.sourceId())))
                .toArray(org.apache.iceberg.types.Type[]::new);
        Optional<PartitionData> partitionData = Optional.empty();
        if (spec.isPartitioned()) {
            verify(partitionDataAsJson.isPresent(), "partitionDataJson is null");
            partitionData = Optional.of(PartitionData.fromJson(partitionDataAsJson.get(), partitionColumnTypes));
        }
        return partitionData;
    }

    public static Optional<PartitionData> partitionDataFromStructLike(PartitionSpec spec, StructLike partition)
    {
        org.apache.iceberg.types.Type[] partitionColumnTypes = spec.fields().stream()
                .map(field -> field.transform().getResultType(
                        spec.schema().findType(field.sourceId())))
                .toArray(org.apache.iceberg.types.Type[]::new);
        Optional<PartitionData> partitionData = Optional.empty();
        if (spec.isPartitioned()) {
            partitionData = Optional.of(PartitionData.fromStructLike(partition, partitionColumnTypes));
        }
        return partitionData;
    }

    /**
     * Get the metadata location for target {@link Table},
     * considering iceberg table properties {@code WRITE_METADATA_LOCATION}
     */
    public static String metadataLocation(Table icebergTable)
    {
        String metadataLocation = icebergTable.properties().get(WRITE_METADATA_LOCATION);

        if (metadataLocation != null) {
            return String.format("%s", LocationUtil.stripTrailingSlash(metadataLocation));
        }
        else {
            return String.format("%s/%s", icebergTable.location(), "metadata");
        }
    }

    /**
     * Get the data location for target {@link Table},
     * considering iceberg table properties {@code WRITE_DATA_LOCATION}, {@code OBJECT_STORE_PATH} and {@code WRITE_FOLDER_STORAGE_LOCATION}
     */
    public static String dataLocation(Table icebergTable)
    {
        Map<String, String> properties = icebergTable.properties();
        String dataLocation = properties.get(WRITE_DATA_LOCATION);
        if (dataLocation == null) {
            dataLocation = properties.get(OBJECT_STORE_PATH);
            if (dataLocation == null) {
                dataLocation = properties.get(WRITE_FOLDER_STORAGE_LOCATION);
                if (dataLocation == null) {
                    dataLocation = String.format("%s/data", icebergTable.location());
                }
            }
        }
        return dataLocation;
    }

    public static Long getSplitSize(Table table)
    {
        return Long.parseLong(table.properties()
                .getOrDefault(SPLIT_SIZE,
                        String.valueOf(SPLIT_SIZE_DEFAULT)));
    }

    public static DataSize getTargetSplitSize(long sessionValueProperty, long icebergScanTargetSplitSize)
    {
        return sessionValueProperty == 0 ?
                succinctBytes(icebergScanTargetSplitSize) :
                succinctBytes(sessionValueProperty);
    }

    public static DataSize getTargetSplitSize(ConnectorSession session, Scan<?, ?, ?> scan)
    {
        return getTargetSplitSize(IcebergSessionProperties.getTargetSplitSize(session), scan.targetSplitSize());
    }

    // This code is copied from Iceberg
    private static String fullTableName(String catalogName, TableIdentifier identifier)
    {
        StringBuilder sb = new StringBuilder();

        if (catalogName.contains("/") || catalogName.contains(":")) {
            // use / for URI-like names: thrift://host:port/db.table
            sb.append(catalogName);
            if (!catalogName.endsWith("/")) {
                sb.append("/");
            }
        }
        else {
            // use . for non-URI named catalogs: prod.db.table
            sb.append(catalogName).append(".");
        }

        for (String level : identifier.namespace().levels()) {
            sb.append(level).append(".");
        }

        sb.append(identifier.name());

        return sb.toString();
    }
}
