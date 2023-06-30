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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.LocationProvider;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.hive.HiveMetadata.TABLE_COMMENT;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableProperties.FORMAT_VERSION;
import static com.facebook.presto.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static com.facebook.presto.iceberg.PartitionFields.toPartitionFields;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.collect.Streams.stream;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.LocationProviders.locationsFor;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;

public final class IcebergUtil
{
    private static final Pattern SIMPLE_NAME = Pattern.compile("[a-z][a-z0-9]*");

    private IcebergUtil() {}

    public static boolean isIcebergTable(com.facebook.presto.hive.metastore.Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    public static Table getHiveIcebergTable(ExtendedHiveMetastore metastore, HdfsEnvironment hdfsEnvironment, ConnectorSession session, SchemaTableName table)
    {
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        TableOperations operations = new HiveTableOperations(
                metastore,
                new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER),
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

    public static ConnectorTableMetadata getTableMetadataFromTable(SchemaTableName tableName, Table icebergTable, TypeManager typeManager)
    {
        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable, typeManager);

        return new ConnectorTableMetadata(tableName, columns, createMetadataProperties(icebergTable), getTableComment(icebergTable));
    }

    public static List<ColumnMetadata> getColumnMetadatas(org.apache.iceberg.Table table, TypeManager typeManager)
    {
        return table.schema().columns().stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.name())
                        .setType(toPrestoType(column.type(), typeManager))
                        .setComment(Optional.ofNullable(column.doc()))
                        .setHidden(false)
                        .build())
                .collect(toImmutableList());
    }

    public static long resolveSnapshotId(Table table, long snapshotId)
    {
        if (table.snapshot(snapshotId) != null) {
            return snapshotId;
        }

        return reverse(table.history()).stream()
                .filter(entry -> entry.timestampMillis() <= snapshotId)
                .map(HistoryEntry::snapshotId)
                .findFirst()
                .orElseThrow(() -> new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, format("Invalid snapshot [%s] for table: %s", snapshotId, table)));
    }

    public static Optional<Long> resolveSnapshotIdByName(Table table, IcebergTableName name)
    {
        if (name.getSnapshotId().isPresent()) {
            if (table.snapshot(name.getSnapshotId().get()) == null) {
                throw new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, format("Invalid snapshot [%s] for table: %s", name.getSnapshotId().get(), table));
            }
            return name.getSnapshotId();
        }
        return Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId);
    }

    public static List<IcebergColumnHandle> getColumns(Schema schema, TypeManager typeManager)
    {
        return schema.columns().stream()
                .map(column -> IcebergColumnHandle.create(column, typeManager))
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

    public static FileFormat getFileFormat(Table table)
    {
        return FileFormat.valueOf(table.properties()
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }

    public static ImmutableMap<String, Object> createMetadataProperties(org.apache.iceberg.Table icebergTable)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));

        int formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        properties.put(FORMAT_VERSION, String.valueOf(formatVersion));

        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        return properties.build();
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
}
