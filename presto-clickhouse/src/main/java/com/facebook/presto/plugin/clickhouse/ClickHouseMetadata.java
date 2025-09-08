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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.airlift.log.Logger;
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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ClickHouseMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(ClickHouseMetadata.class);
    private final ClickHouseClient clickHouseClient;
    private final boolean allowDropTable;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public ClickHouseMetadata(ClickHouseClient clickHouseClient, boolean allowDropTable)
    {
        this.clickHouseClient = requireNonNull(clickHouseClient, "client is null");
        this.allowDropTable = allowDropTable;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return clickHouseClient.schemaExists(ClickHouseIdentity.from(session), schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(clickHouseClient.getSchemaNames(ClickHouseIdentity.from(session)));
    }

    @Override
    public ClickHouseTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return clickHouseClient.getTableHandle(ClickHouseIdentity.from(session), tableName);
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        ClickHouseTableHandle tableHandle = (ClickHouseTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ClickHouseTableLayoutHandle(tableHandle, constraint.getSummary(), Optional.empty(), Optional.empty(), Optional.empty()));
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ClickHouseTableHandle handle = (ClickHouseTableHandle) table;

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (ClickHouseColumnHandle column : clickHouseClient.getColumns(session, handle)) {
            columnMetadata.add(column.getColumnMetadata(normalizeIdentifier(session, column.getColumnName())));
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return clickHouseClient.getTableNames(ClickHouseIdentity.from(session), schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ClickHouseTableHandle clickHouseTableHandle = (ClickHouseTableHandle) tableHandle;

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ClickHouseColumnHandle column : clickHouseClient.getColumns(session, clickHouseTableHandle)) {
            columnHandles.put(normalizeIdentifier(session, column.getColumnMetadata(column.getColumnName()).getName()), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables;
        if (prefix.getTableName() != null) {
            tables = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tables = listTables(session, prefix.getSchemaName());
        }
        for (SchemaTableName tableName : tables) {
            try {
                ClickHouseTableHandle tableHandle = clickHouseClient.getTableHandle(ClickHouseIdentity.from(session), tableName);
                if (tableHandle == null) {
                    continue;
                }
                columns.put(tableName, getTableMetadata(session, tableHandle).getColumns());
            }
            catch (TableNotFoundException e) {
                log.info("table disappeared during listing operation");
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ClickHouseColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this catalog");
        }
        ClickHouseTableHandle handle = (ClickHouseTableHandle) tableHandle;
        clickHouseClient.dropTable(ClickHouseIdentity.from(session), handle);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        ClickHouseOutputTableHandle handle = clickHouseClient.beginCreateTable(session, tableMetadata);
        setRollback(() -> clickHouseClient.rollbackCreateTable(ClickHouseIdentity.from(session), handle));
        return handle;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        clickHouseClient.createTable(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        ClickHouseOutputTableHandle handle = (ClickHouseOutputTableHandle) tableHandle;
        clickHouseClient.commitCreateTable(ClickHouseIdentity.from(session), handle);
        clearRollback();
        return Optional.empty();
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ClickHouseOutputTableHandle handle = clickHouseClient.beginInsertTable(session, getTableMetadata(session, tableHandle));
        setRollback(() -> clickHouseClient.rollbackCreateTable(ClickHouseIdentity.from(session), handle));
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        ClickHouseOutputTableHandle clickHouseInsertHandle = (ClickHouseOutputTableHandle) tableHandle;
        clickHouseClient.finishInsertTable(ClickHouseIdentity.from(session), clickHouseInsertHandle);
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata)
    {
        ClickHouseTableHandle tableHandle = (ClickHouseTableHandle) table;
        clickHouseClient.addColumn(ClickHouseIdentity.from(session), tableHandle, columnMetadata);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column)
    {
        ClickHouseTableHandle tableHandle = (ClickHouseTableHandle) table;
        ClickHouseColumnHandle columnHandle = (ClickHouseColumnHandle) column;
        clickHouseClient.dropColumn(ClickHouseIdentity.from(session), tableHandle, columnHandle);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column, String target)
    {
        ClickHouseTableHandle tableHandle = (ClickHouseTableHandle) table;
        ClickHouseColumnHandle columnHandle = (ClickHouseColumnHandle) column;
        clickHouseClient.renameColumn(ClickHouseIdentity.from(session), tableHandle, columnHandle, target);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle table, SchemaTableName newTableName)
    {
        ClickHouseTableHandle tableHandle = (ClickHouseTableHandle) table;
        clickHouseClient.renameTable(ClickHouseIdentity.from(session), tableHandle, newTableName);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        ClickHouseTableHandle handle = (ClickHouseTableHandle) tableHandle;
        List<ClickHouseColumnHandle> columns = columnHandles.stream().map(ClickHouseColumnHandle.class::cast).collect(Collectors.toList());
        return clickHouseClient.getTableStatistics(session, handle, columns, constraint.getSummary());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        clickHouseClient.createSchema(ClickHouseIdentity.from(session), schemaName, properties);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        clickHouseClient.dropSchema(ClickHouseIdentity.from(session), schemaName);
    }
}
