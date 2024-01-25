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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
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
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.SnapshotUtil;
import org.joda.time.DateTimeZone;

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
import java.util.regex.Pattern;
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
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.charPartitionKey;
import static com.facebook.presto.hive.HiveUtil.datePartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.hive.HiveUtil.floatPartitionKey;
import static com.facebook.presto.hive.HiveUtil.integerPartitionKey;
import static com.facebook.presto.hive.HiveUtil.longDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.shortDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.smallintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.hive.HiveUtil.tinyintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.varcharPartitionKey;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_QUERY_ID_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VERSION_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VIEW_COMMENT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.metastore.MetastoreUtil.TABLE_COMMENT;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_PARTITION_VALUE;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_TABLE_TIMESTAMP;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isMergeOnReadModeEnabled;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static com.google.common.collect.Streams.stream;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Comparator.comparing;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES;
import static org.apache.iceberg.LocationProviders.locationsFor;
import static org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;
import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.FIXED;

public final class IcebergUtil
{
    private static final Pattern SIMPLE_NAME = Pattern.compile("[a-z][a-z0-9]*");
    private static final Logger log = Logger.get(IcebergUtil.class);

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

    public static Table getHiveIcebergTable(ExtendedHiveMetastore metastore, HdfsEnvironment hdfsEnvironment, ConnectorSession session, SchemaTableName table)
    {
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        TableOperations operations = new HiveTableOperations(
                metastore,
                new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, session.getWarningCollector()),
                hdfsEnvironment,
                hdfsContext,
                table.getSchemaName(),
                table.getTableName());
        return new BaseTable(operations, quotedTableName(table));
    }

    public static Table getNativeIcebergTable(IcebergResourceFactory resourceFactory, ConnectorSession session, SchemaTableName table)
    {
        return resourceFactory.getCatalog(session).loadTable(toIcebergTableIdentifier(table));
    }

    public static List<IcebergColumnHandle> getPartitionKeyColumnHandles(org.apache.iceberg.Table table, TypeManager typeManager)
    {
        ImmutableList.Builder<IcebergColumnHandle> partitionColumns = ImmutableList.builder();
        List<IcebergColumnHandle> allColumns = getColumns(table.schema(), table.spec(), typeManager);

        for (int i = 0; i < table.spec().fields().size(); i++) {
            PartitionField field = table.spec().fields().get(i);
            if (field.transform().toString().equals("identity")) {
                Optional<IcebergColumnHandle> columnHandle = allColumns.stream().filter(icebergColumnHandle -> Objects.equals(icebergColumnHandle.getName(), field.name())).findAny();
                columnHandle.ifPresent(partitionColumns::add);
            }
        }

        return partitionColumns.build();
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

    public static long getSnapshotIdAsOfTime(Table table, long millisUtc)
    {
        return table.history().stream()
                .filter(logEntry -> logEntry.timestampMillis() <= millisUtc)
                .max(comparing(HistoryEntry::timestampMillis))
                .orElseThrow(() -> new PrestoException(ICEBERG_INVALID_TABLE_TIMESTAMP, format("No history found based on timestamp for table %s", table.name())))
                .snapshotId();
    }

    public static List<IcebergColumnHandle> getColumns(Schema schema, PartitionSpec partitionSpec, TypeManager typeManager)
    {
        List<String> partitionFieldNames = new ArrayList<>();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().toString().equals("identity")) {
                partitionFieldNames.add(field.name());
            }
        }

        return schema.columns().stream()
                .map(column -> partitionFieldNames.contains(column.name()) ? IcebergColumnHandle.create(column, typeManager, PARTITION_KEY) : IcebergColumnHandle.create(column, typeManager, REGULAR))
                .collect(toImmutableList());
    }

    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().toString().equals("identity")) {
                columns.put(field, i);
            }
        }
        return columns.build();
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

    private static String quotedTableName(SchemaTableName name)
    {
        return quotedName(name.getSchemaName()) + "." + quotedName(name.getTableName());
    }

    private static String quotedName(String name)
    {
        if (SIMPLE_NAME.matcher(name).matches()) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
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

    private static boolean isValidPartitionType(com.facebook.presto.common.type.Type type)
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
                TIMESTAMP.equals(type) ||
                VARBINARY.equals(type) ||
                isVarcharType(type) ||
                isCharType(type);
    }

    private static void verifyPartitionTypeSupported(String partitionName, com.facebook.presto.common.type.Type type)
    {
        if (!isValidPartitionType(type)) {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported type [%s] for partition: %s", type, partitionName));
        }
    }

    private static NullableValue parsePartitionValue(
            String partitionName,
            String value,
            com.facebook.presto.common.type.Type type,
            DateTimeZone timeZone)
    {
        verifyPartitionTypeSupported(partitionName, type);

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (value == null) {
                return NullableValue.asNull(decimalType);
            }
            if (decimalType.isShort()) {
                if (value.isEmpty()) {
                    return NullableValue.of(decimalType, 0L);
                }
                return NullableValue.of(decimalType, shortDecimalPartitionKey(value, decimalType, partitionName));
            }
            else {
                if (value.isEmpty()) {
                    return NullableValue.of(decimalType, Decimals.encodeUnscaledValue(BigInteger.ZERO));
                }
                return NullableValue.of(decimalType, longDecimalPartitionKey(value, decimalType, partitionName));
            }
        }

        if (BOOLEAN.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(BOOLEAN);
            }
            if (value.isEmpty()) {
                return NullableValue.of(BOOLEAN, false);
            }
            return NullableValue.of(BOOLEAN, booleanPartitionKey(value, partitionName));
        }

        if (TINYINT.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(TINYINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(TINYINT, 0L);
            }
            return NullableValue.of(TINYINT, tinyintPartitionKey(value, partitionName));
        }

        if (SMALLINT.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(SMALLINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(SMALLINT, 0L);
            }
            return NullableValue.of(SMALLINT, smallintPartitionKey(value, partitionName));
        }

        if (INTEGER.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(INTEGER);
            }
            if (value.isEmpty()) {
                return NullableValue.of(INTEGER, 0L);
            }
            return NullableValue.of(INTEGER, integerPartitionKey(value, partitionName));
        }

        if (BIGINT.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(BIGINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(BIGINT, 0L);
            }
            return NullableValue.of(BIGINT, bigintPartitionKey(value, partitionName));
        }

        if (DATE.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(DATE);
            }
            return NullableValue.of(DATE, datePartitionKey(value, partitionName));
        }

        if (TIMESTAMP.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(TIMESTAMP);
            }
            return NullableValue.of(TIMESTAMP, timestampPartitionKey(value, timeZone, partitionName));
        }

        if (REAL.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(REAL);
            }
            if (value.isEmpty()) {
                return NullableValue.of(REAL, (long) floatToRawIntBits(0.0f));
            }
            return NullableValue.of(REAL, floatPartitionKey(value, partitionName));
        }

        if (DOUBLE.equals(type)) {
            if (value == null) {
                return NullableValue.asNull(DOUBLE);
            }
            if (value.isEmpty()) {
                return NullableValue.of(DOUBLE, 0.0);
            }
            return NullableValue.of(DOUBLE, doublePartitionKey(value, partitionName));
        }

        if (isVarcharType(type)) {
            if (value == null) {
                return NullableValue.asNull(type);
            }
            return NullableValue.of(type, varcharPartitionKey(value, partitionName, type));
        }

        if (isCharType(type)) {
            if (value == null) {
                return NullableValue.asNull(type);
            }
            return NullableValue.of(type, charPartitionKey(value, partitionName, type));
        }

        throw new VerifyException(format("Unhandled type [%s] for partition: %s", type, partitionName));
    }

    public static List<HivePartition> getPartitions(
            TypeManager typeManager,
            ConnectorTableHandle tableHandle,
            Table icebergTable,
            Constraint<ColumnHandle> constraint,
            List<IcebergColumnHandle> partitionColumns)
    {
        IcebergTableName name = ((IcebergTableHandle) tableHandle).getIcebergTableName();

        // Empty iceberg table would cause `snapshotId` not present
        Optional<Long> snapshotId = resolveSnapshotIdByName(icebergTable, name);
        if (!snapshotId.isPresent()) {
            return ImmutableList.of();
        }

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(constraint.getSummary().simplify().transform(IcebergColumnHandle.class::cast)))
                .useSnapshot(snapshotId.get());

        Set<HivePartition> partitions = new HashSet<>();

        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
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

                    NullableValue partitionValue = parsePartitionValue(partition.toString(), partitionStringValue, toPrestoType(type, typeManager), DateTimeZone.UTC);
                    Optional<IcebergColumnHandle> column = partitionColumns.stream()
                            .filter(icebergColumnHandle -> Objects.equals(icebergColumnHandle.getName(), field.name()))
                            .findAny();

                    builder.put(column.get(), partitionValue);
                });

                Map<ColumnHandle, NullableValue> values = builder.build();
                HivePartition newPartition = new HivePartition(
                        ((IcebergTableHandle) tableHandle).getSchemaTableName(),
                        partition.toString(),
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
            if (type.equals(DATE) || type.equals(TIME) || type.equals(TIMESTAMP)) {
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

    public static void loadCachingProperties(Map<String, String> properties, IcebergConfig icebergConfig)
    {
        properties.put(IO_MANIFEST_CACHE_ENABLED, "true");
        properties.put(IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, String.valueOf(icebergConfig.getMaxManifestCacheSize()));
        properties.put(IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH, String.valueOf(icebergConfig.getManifestCacheMaxContentLength()));
        properties.put(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS, String.valueOf(icebergConfig.getManifestCacheExpireDuration()));
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
                    if (shouldIncludeFile(deleteFile)) {
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

        private boolean shouldIncludeFile(DeleteFile file)
        {
            boolean matchesPartition = !requestedPartitionSpec.isPresent() ||
                    requestedPartitionSpec.get().equals(partitionSpecsById.get(file.specId()).fields().stream().map(PartitionField::fieldId).collect(Collectors.toSet()));
            return matchesPartition && (file.content() == FileContent.POSITION_DELETES ||
                    !requestedSchema.isPresent() || requestedSchema.get().equals(ImmutableSet.copyOf(file.equalityFieldIds())));
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
}
