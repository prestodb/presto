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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
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
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class LanceMetadata
        implements ConnectorMetadata
{
    public static final String LANCE_DEFAULT_SCHEMA = "default";

    private final LanceNamespaceHolder namespaceHolder;
    private final JsonCodec<LanceCommitTaskData> commitTaskDataCodec;

    @Inject
    public LanceMetadata(
            LanceNamespaceHolder namespaceHolder,
            JsonCodec<LanceCommitTaskData> commitTaskDataCodec)
    {
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
        this.commitTaskDataCodec = requireNonNull(commitTaskDataCodec, "commitTaskDataCodec is null");
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
        if (!schemaExists(session, tableName.getSchemaName())) {
            return null;
        }
        if (!namespaceHolder.tableExists(tableName.getSchemaName(), tableName.getTableName())) {
            return null;
        }
        return new LanceTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LanceTableHandle lanceTable = (LanceTableHandle) table;
        if (!namespaceHolder.tableExists(lanceTable.getSchemaName(), lanceTable.getTableName())) {
            return null;
        }
        Schema arrowSchema = namespaceHolder.describeTable(lanceTable.getSchemaName(), lanceTable.getTableName());
        SchemaTableName schemaTableName = new SchemaTableName(lanceTable.getSchemaName(), lanceTable.getTableName());

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (Field field : arrowSchema.getFields()) {
            columnsMetadata.add(ColumnMetadata.builder()
                    .setName(field.getName())
                    .setType(LanceColumnHandle.toPrestoType(field))
                    .setNullable(field.isNullable())
                    .build());
        }

        return new ConnectorTableMetadata(schemaTableName, columnsMetadata.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schema = schemaName.orElse(LANCE_DEFAULT_SCHEMA);
        return namespaceHolder.listTables(schema).stream()
                .map(tableName -> new SchemaTableName(schema, tableName))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTable = (LanceTableHandle) tableHandle;
        if (!namespaceHolder.tableExists(lanceTable.getSchemaName(), lanceTable.getTableName())) {
            return ImmutableMap.of();
        }
        Schema arrowSchema = namespaceHolder.describeTable(lanceTable.getSchemaName(), lanceTable.getTableName());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (Field field : arrowSchema.getFields()) {
            LanceColumnHandle columnHandle = new LanceColumnHandle(
                    field.getName(),
                    LanceColumnHandle.toPrestoType(field),
                    field.isNullable());
            columnHandles.put(field.getName(), columnHandle);
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((LanceColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTableName() != null
                ? singletonList(prefix.toSchemaTableName())
                : listTables(session, Optional.ofNullable(prefix.getSchemaName()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : tables) {
            ConnectorTableHandle tableHandle = getTableHandle(session, tableName);
            if (tableHandle != null) {
                ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
                if (tableMetadata != null) {
                    columns.put(tableName, tableMetadata.getColumns());
                }
            }
        }
        return columns.build();
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        LanceTableHandle lanceTable = (LanceTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new LanceTableLayoutHandle(lanceTable, constraint.getSummary()));
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorNewTableLayout> layout)
    {
        Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns());

        namespaceHolder.createTable(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                arrowSchema);

        List<LanceColumnHandle> columns = tableMetadata.getColumns().stream()
                .map(col -> new LanceColumnHandle(col.getName(), col.getType(), col.isNullable()))
                .collect(toImmutableList());

        return new LanceWritableTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                arrowSchema.toJson(),
                columns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        LanceWritableTableHandle handle = (LanceWritableTableHandle) tableHandle;

        if (!fragments.isEmpty()) {
            List<org.lance.FragmentMetadata> allFragments = collectFragments(fragments);
            namespaceHolder.commitAppend(handle.getSchemaName(), handle.getTableName(), allFragments);
        }
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTable = (LanceTableHandle) tableHandle;
        Schema arrowSchema = namespaceHolder.describeTable(lanceTable.getSchemaName(), lanceTable.getTableName());

        List<LanceColumnHandle> columns = arrowSchema.getFields().stream()
                .map(field -> new LanceColumnHandle(
                        field.getName(),
                        LanceColumnHandle.toPrestoType(field),
                        field.isNullable()))
                .collect(toImmutableList());

        return new LanceWritableTableHandle(
                lanceTable.getSchemaName(),
                lanceTable.getTableName(),
                arrowSchema.toJson(),
                columns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        LanceWritableTableHandle handle = (LanceWritableTableHandle) tableHandle;

        if (!fragments.isEmpty()) {
            List<org.lance.FragmentMetadata> allFragments = collectFragments(fragments);
            namespaceHolder.commitAppend(handle.getSchemaName(), handle.getTableName(), allFragments);
        }
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTable = (LanceTableHandle) tableHandle;
        namespaceHolder.dropTable(lanceTable.getSchemaName(), lanceTable.getTableName());
    }

    private List<org.lance.FragmentMetadata> collectFragments(Collection<Slice> fragments)
    {
        ImmutableList.Builder<org.lance.FragmentMetadata> allFragments = ImmutableList.builder();
        for (Slice slice : fragments) {
            LanceCommitTaskData commitData = commitTaskDataCodec.fromJson(slice.getBytes());
            allFragments.addAll(LancePageSink.deserializeFragments(commitData.getFragmentsJson()));
        }
        return allFragments.build();
    }
}
