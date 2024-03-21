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
package com.facebook.presto.lance.metadata;

import com.facebook.presto.lance.client.LanceClient;
import com.facebook.presto.lance.ingestion.LanceIngestionTableHandle;
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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.lancedb.lance.FragmentMetadata;
import io.airlift.slice.Slice;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class LanceMetadata
        implements ConnectorMetadata
{
    public static final String LANCE_DEFAULT_SCHEMA = "default";
    private final LanceClient lanceClient;

    @Inject
    public LanceMetadata(LanceClient client)
    {
        lanceClient = requireNonNull(client, "client is null");
    }
    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return LANCE_DEFAULT_SCHEMA.equals(schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(LANCE_DEFAULT_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(session, tableName, Optional.empty());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> tableVersion)
    {
        try {
            if(lanceClient.getConn().tableNames().contains(tableName.getTableName())) {
                return new LanceTableHandle(tableName.getSchemaName(), tableName.getTableName());
            }
            return null;
        }
        catch (Exception ex) {
            return null;
        }
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;
        Schema arrowSchema = lanceClient.getSchema(((LanceTableHandle) table).getTableName());
        SchemaTableName schemaTableName =
                new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName());
        ImmutableList.Builder<ColumnMetadata> columnsMetadataListBuilder =
                ImmutableList.builderWithExpectedSize(arrowSchema.getFields().size());
        for (Field field : arrowSchema.getFields()) {
            columnsMetadataListBuilder.add(new ColumnMetadata(field.getName(),
                    LanceColumnType.fromArrowType(field.getType()).getPrestoType()));
        }
        return new ConnectorTableMetadata(schemaTableName, columnsMetadataListBuilder.build());
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        return ConnectorMetadata.super.getInfo(layoutHandle);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return lanceClient.getConn().tableNames().stream()
                .map(tableName -> new SchemaTableName(schemaName.orElse(LANCE_DEFAULT_SCHEMA), tableName))
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        Schema arrowSchema = lanceClient.getSchema(lanceTableHandle.getTableName());
        ImmutableMap.Builder<String, ColumnHandle> columnHandleMapBuilder = ImmutableMap.builder();
        for (Field field : arrowSchema.getFields()) {
            LanceColumnHandle columnHandle =
                    new LanceColumnHandle(field.getName(), LanceColumnType.fromArrowType(field.getType()).getPrestoType());
            columnHandleMapBuilder.put(field.getName(), columnHandle);
        }
        return columnHandleMapBuilder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        LanceColumnHandle lanceColumnHandle = (LanceColumnHandle) columnHandle;
        return new ColumnMetadata(lanceColumnHandle.getColumnName(), lanceColumnHandle.getColumnType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTableName() != null ?
                singletonList(prefix.toSchemaTableName()) : listTables(session, Optional.ofNullable(prefix.getSchemaName()));
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : tables) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, getTableHandle(session, tableName));
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        ImmutableList.Builder<Field> arrowFieldBuilder = ImmutableList.builder();
        ImmutableList.Builder<LanceColumnInfo> columnInfoBuilder = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            LanceColumnType lanceColumnType = LanceColumnType.fromPrestoType(column.getType());
            arrowFieldBuilder.add(Field.nullable(column.getName(), lanceColumnType.getArrowType()));
            columnInfoBuilder.add(new LanceColumnInfo(column.getName(), lanceColumnType));
        }
        Schema schema = new Schema(arrowFieldBuilder.build());
        lanceClient.createTable(tableMetadata.getTable().getTableName(), schema);
        return new LanceIngestionTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnInfoBuilder.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        LanceIngestionTableHandle lanceTableHandle = (LanceIngestionTableHandle) tableHandle;
        List<FragmentMetadata> fragmentMetadataList = fragments.stream()
                .map(fragmentSlice -> FragmentMetadata.fromJson(new String(fragmentSlice.getBytes())))
                .collect(Collectors.toList());
        long tableReadVersion = lanceClient.getTableVersion(lanceTableHandle.getTableName());
        lanceClient.appendAndCommit(lanceTableHandle.getTableName(), fragmentMetadataList, tableReadVersion);
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        Schema arrowSchema = lanceClient.getSchema(lanceTableHandle.getTableName());
        ImmutableList.Builder<LanceColumnInfo> columnsInfoListBuilder =
                ImmutableList.builderWithExpectedSize(arrowSchema.getFields().size());
        for (Field field : arrowSchema.getFields()) {
            columnsInfoListBuilder.add(new LanceColumnInfo(field.getName(),
                    LanceColumnType.fromArrowType(field.getType())));
        }
        return new LanceIngestionTableHandle(
                lanceTableHandle.getSchemaName(),
                lanceTableHandle.getTableName(),
                columnsInfoListBuilder.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        LanceIngestionTableHandle lanceTableHandle = (LanceIngestionTableHandle) tableHandle;
        List<FragmentMetadata> fragmentMetadataList = fragments.stream()
                .map(fragmentSlice -> FragmentMetadata.fromJson(new String(fragmentSlice.getBytes())))
                .collect(Collectors.toList());
        long tableReadVersion = lanceClient.getTableVersion(lanceTableHandle.getTableName());
        lanceClient.appendAndCommit(lanceTableHandle.getTableName(), fragmentMetadataList, tableReadVersion);
        return Optional.empty();
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new LanceTableLayoutHandle(lanceTableHandle, constraint.getSummary()));
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }
}
