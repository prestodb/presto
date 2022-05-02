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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.iceberg.util.IcebergPrestoModelConverters;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergTableProperties.getFileFormat;
import static com.facebook.presto.iceberg.IcebergTableProperties.getFormatVersion;
import static com.facebook.presto.iceberg.IcebergTableProperties.getPartitioning;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getTableComment;
import static com.facebook.presto.iceberg.IcebergUtil.resolveSnapshotIdByName;
import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.facebook.presto.iceberg.TableType.DATA;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergNamespace;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

public class IcebergNativeMetadata
        extends IcebergAbstractMetadata
{
    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final String TABLE_COMMENT = "comment";

    private final IcebergResourceFactory resourceFactory;
    private final CatalogType catalogType;

    public IcebergNativeMetadata(
            IcebergResourceFactory resourceFactory,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            CatalogType catalogType)
    {
        super(typeManager, commitTaskCodec);
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        SupportsNamespaces supportsNamespaces = resourceFactory.getNamespaces(session);
        return supportsNamespaces.listNamespaces()
                .stream()
                .map(IcebergPrestoModelConverters::toPrestoSchemaName)
                .collect(toList());
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        verify(name.getTableType() == DATA, "Wrong table type: " + name.getTableType());
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(tableName.getSchemaName(), name.getTableName());

        Table table;
        try {
            table = resourceFactory.getCatalog(session).loadTable(tableIdentifier);
        }
        catch (NoSuchTableException e) {
            // return null to throw
            return null;
        }

        return new IcebergTableHandle(
                tableName.getSchemaName(),
                name.getTableName(),
                name.getTableType(),
                resolveSnapshotIdByName(table, name),
                TupleDomain.all());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        if (name.getTableType() == DATA) {
            return Optional.empty();
        }

        TableIdentifier tableIdentifier = toIcebergTableIdentifier(tableName.getSchemaName(), name.getTableName());
        Table table;
        try {
            table = resourceFactory.getCatalog(session).loadTable(tableIdentifier);
        }
        catch (NoSuchTableException e) {
            return Optional.empty();
        }

        if (name.getSnapshotId().isPresent() && table.snapshot(name.getSnapshotId().get()) == null) {
            throw new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, format("Invalid snapshot [%s] for table: %s", name.getSnapshotId().get(), table));
        }

        return getIcebergSystemTable(tableName, table);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && INFORMATION_SCHEMA.equals(schemaName.get())) {
            return listSchemaNames(session).stream()
                    .map(schema -> new SchemaTableName(INFORMATION_SCHEMA, schema))
                    .collect(toList());
        }

        return resourceFactory.getCatalog(session).listTables(toIcebergNamespace(schemaName))
                .stream()
                .map(IcebergPrestoModelConverters::toPrestoSchemaTableName)
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = getNativeIcebergTable(resourceFactory, session, table.getSchemaTableName());
        return getColumns(icebergTable.schema(), typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setComment(column.getComment())
                .setHidden(false)
                .build();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        resourceFactory.getNamespaces(session).createNamespace(toIcebergNamespace(Optional.of(schemaName)),
                properties.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        try {
            resourceFactory.getNamespaces(session).dropNamespace(toIcebergNamespace(Optional.of(schemaName)));
        }
        catch (NamespaceNotEmptyException e) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, format("Iceberg %s catalog does not support rename namespace", catalogType.name()));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = toIcebergSchema(tableMetadata.getColumns());

        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        FileFormat fileFormat = getFileFormat(tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toString());
        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }
        String formatVersion = getFormatVersion(tableMetadata.getProperties());
        if (formatVersion != null) {
            propertiesBuilder.put(FORMAT_VERSION, formatVersion);
        }

        try {
            transaction = resourceFactory.getCatalog(session).newCreateTableTransaction(
                    toIcebergTableIdentifier(schemaTableName), schema, partitionSpec, propertiesBuilder.build());
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        Table icebergTable = transaction.table();
        return new IcebergWritableTableHandle(
                schemaName,
                tableName,
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), typeManager),
                icebergTable.location(),
                fileFormat,
                icebergTable.properties());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = getNativeIcebergTable(resourceFactory, session, table.getSchemaTableName());

        return beginIcebergTableInsert(table, icebergTable);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(((IcebergTableHandle) tableHandle).getSchemaTableName());
        resourceFactory.getCatalog(session).dropTable(tableIdentifier);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        TableIdentifier from = toIcebergTableIdentifier(((IcebergTableHandle) tableHandle).getSchemaTableName());
        TableIdentifier to = toIcebergTableIdentifier(newTable);
        resourceFactory.getCatalog(session).renameTable(from, to);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(((IcebergTableHandle) tableHandle).getSchemaTableName());
        Table icebergTable = resourceFactory.getCatalog(session).loadTable(tableIdentifier);
        icebergTable.updateSchema().addColumn(column.getName(), toIcebergType(column.getType()), column.getComment()).commit();
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(((IcebergTableHandle) tableHandle).getSchemaTableName());
        Table icebergTable = resourceFactory.getCatalog(session).loadTable(tableIdentifier);
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(((IcebergTableHandle) tableHandle).getSchemaTableName());
        Table icebergTable = resourceFactory.getCatalog(session).loadTable(tableIdentifier);
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    @Override
    protected ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table)
    {
        Table icebergTable;
        try {
            icebergTable = getNativeIcebergTable(resourceFactory, session, table);
        }
        catch (NoSuchTableException e) {
            throw new TableNotFoundException(table);
        }

        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);

        return new ConnectorTableMetadata(table, columns, createMetadataProperties(icebergTable), getTableComment(icebergTable));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = getNativeIcebergTable(resourceFactory, session, handle.getSchemaTableName());
        return TableStatisticsMaker.getTableStatistics(typeManager, constraint, handle, icebergTable);
    }
}
