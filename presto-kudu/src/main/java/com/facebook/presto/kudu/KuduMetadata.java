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
package com.facebook.presto.kudu;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.kudu.properties.KuduTableProperties;
import com.facebook.presto.kudu.properties.PartitionDesign;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduTable;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class KuduMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final KuduClientSession clientSession;

    @Inject
    public KuduMetadata(KuduConnectorId connectorId, KuduClientSession clientSession)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return clientSession.listSchemaNames();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return clientSession.listTables(schemaName);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "SchemaTablePrefix is null");

        List<SchemaTableName> tables;
        if (prefix.getTableName() == null) {
            tables = listTables(session, prefix.getSchemaName());
        }
        else {
            tables = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : tables) {
            KuduTableHandle tableHandle = getTableHandle(session, tableName);
            if (tableHandle != null) {
                ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ColumnMetadata getColumnMetadata(ColumnSchema column)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        StringBuilder extra = new StringBuilder();
        if (column.isKey()) {
            properties.put(KuduTableProperties.PRIMARY_KEY, true);
            extra.append("primary_key, ");
        }

        if (column.isNullable()) {
            properties.put(KuduTableProperties.NULLABLE, true);
            extra.append("nullable, ");
        }

        String encoding = KuduTableProperties.lookupEncodingString(column.getEncoding());
        if (!column.getEncoding().equals(ColumnSchema.Encoding.AUTO_ENCODING)) {
            properties.put(KuduTableProperties.ENCODING, encoding);
        }
        extra.append("encoding=").append(encoding).append(", ");

        String compression = KuduTableProperties.lookupCompressionString(column.getCompressionAlgorithm());
        if (!column.getCompressionAlgorithm().equals(ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION)) {
            properties.put(KuduTableProperties.COMPRESSION, compression);
        }
        extra.append("compression=").append(compression);

        Type prestoType = TypeHelper.fromKuduColumn(column);
        return new ColumnMetadata(column.getName(), prestoType, null, extra.toString(), false, properties);
    }

    private ConnectorTableMetadata getTableMetadata(KuduTableHandle tableHandle)
    {
        KuduTable table = tableHandle.getTable(clientSession);
        Schema schema = table.getSchema();

        List<ColumnMetadata> columnsMetaList = schema.getColumns().stream()
                .filter(column -> !column.isKey() || !column.getName().equals(KuduColumnHandle.ROW_ID))
                .map(this::getColumnMetadata)
                .collect(toImmutableList());

        Map<String, Object> properties = clientSession.getTableProperties(tableHandle);
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columnsMetaList, properties);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        KuduTableHandle tableHandle = (KuduTableHandle) connectorTableHandle;
        Schema schema = clientSession.getTableSchema(tableHandle);

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < schema.getColumnCount(); ordinal++) {
            ColumnSchema col = schema.getColumnByIndex(ordinal);
            String name = col.getName();
            Type type = TypeHelper.fromKuduColumn(col);
            KuduColumnHandle columnHandle = new KuduColumnHandle(name, ordinal, type);
            columnHandles.put(name, columnHandle);
        }

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) columnHandle;
        if (kuduColumnHandle.isVirtualRowId()) {
            return new ColumnMetadata(KuduColumnHandle.ROW_ID, VarbinaryType.VARBINARY, null, true);
        }
        else {
            return kuduColumnHandle.getColumnMetadata();
        }
    }

    @Override
    public KuduTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            KuduTable table = clientSession.openTable(schemaTableName);
            return new KuduTableHandle(connectorId, schemaTableName, table);
        }
        catch (NotFoundException e) {
            return null;
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        KuduTableHandle handle = (KuduTableHandle) tableHandle;
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new KuduTableLayoutHandle(handle, constraint.getSummary(), desiredColumns));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        return getTableMetadata(kuduTableHandle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        clientSession.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        clientSession.dropSchema(schemaName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        clientSession.createTable(tableMetadata, ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        clientSession.dropTable(kuduTableHandle.getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        clientSession.renameTable(kuduTableHandle.getSchemaTableName(), newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        clientSession.addColumn(kuduTableHandle.getSchemaTableName(), column);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) column;
        clientSession.dropColumn(kuduTableHandle.getSchemaTableName(), kuduColumnHandle.getName());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        KuduTableHandle kuduTableHandle = (KuduTableHandle) tableHandle;
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) source;
        clientSession.renameColumn(kuduTableHandle.getSchemaTableName(), kuduColumnHandle.getName(), target);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        KuduTableHandle tableHandle = (KuduTableHandle) connectorTableHandle;

        KuduTable table = tableHandle.getTable(clientSession);
        Schema schema = table.getSchema();

        List<ColumnSchema> columns = schema.getColumns();
        List<Type> columnTypes = columns.stream()
                .map(TypeHelper::fromKuduColumn).collect(toImmutableList());

        return new KuduInsertTableHandle(
                connectorId,
                tableHandle.getSchemaTableName(),
                columnTypes,
                table);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorNewTableLayout> layout)
    {
        PartitionDesign design = KuduTableProperties.getPartitionDesign(tableMetadata.getProperties());
        boolean generateUUID = !design.hasPartitions();
        ConnectorTableMetadata finalTableMetadata = tableMetadata;
        if (generateUUID) {
            String rowId = KuduColumnHandle.ROW_ID;
            List<ColumnMetadata> copy = new ArrayList<>(tableMetadata.getColumns());
            Map<String, Object> columnProperties = new HashMap<>();
            columnProperties.put(KuduTableProperties.PRIMARY_KEY, true);
            copy.add(0, new ColumnMetadata(rowId, VarcharType.VARCHAR, "key=true", null, true, columnProperties));
            List<ColumnMetadata> finalColumns = ImmutableList.copyOf(copy);
            Map<String, Object> propsCopy = new HashMap<>(tableMetadata.getProperties());
            propsCopy.put(KuduTableProperties.PARTITION_BY_HASH_COLUMNS, ImmutableList.of(rowId));
            propsCopy.put(KuduTableProperties.PARTITION_BY_HASH_BUCKETS, 2);
            Map<String, Object> finalProperties = ImmutableMap.copyOf(propsCopy);
            finalTableMetadata = new ConnectorTableMetadata(tableMetadata.getTable(),
                    finalColumns, finalProperties, tableMetadata.getComment());
        }
        KuduTable table = clientSession.createTable(finalTableMetadata, false);

        Schema schema = table.getSchema();

        List<ColumnSchema> columns = schema.getColumns();
        List<Type> columnTypes = columns.stream()
                .map(TypeHelper::fromKuduColumn).collect(toImmutableList());
        List<Type> columnOriginalTypes = finalTableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType).collect(toImmutableList());

        return new KuduOutputTableHandle(
                connectorId,
                finalTableMetadata.getTable(),
                columnOriginalTypes,
                columnTypes,
                generateUUID,
                table);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return KuduColumnHandle.ROW_ID_HANDLE;
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        return false;
    }
}
