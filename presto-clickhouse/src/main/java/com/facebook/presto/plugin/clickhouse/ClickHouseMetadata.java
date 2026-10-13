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
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
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
import jakarta.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public class ClickHouseMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(ClickHouseMetadata.class);
    private final ClickHouseClient clickHouseClient;
    private final boolean allowDropTable;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();
    private final ClickHouseConfig clickHouseConfig;

    @Inject
    public ClickHouseMetadata(ClickHouseClient clickHouseClient, boolean allowDropTable, ClickHouseConfig clickHouseConfig)
    {
        this.clickHouseClient = requireNonNull(clickHouseClient, "client is null");
        this.allowDropTable = allowDropTable;
        this.clickHouseConfig = clickHouseConfig;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return clickHouseClient.schemaExists(session, JdbcIdentity.from(session), schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(clickHouseClient.getSchemaNames(session, JdbcIdentity.from(session)));
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return clickHouseClient.getTableHandle(session, JdbcIdentity.from(session), tableName);
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
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
        JdbcTableHandle handle = (JdbcTableHandle) table;

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (JdbcColumnHandle column : clickHouseClient.getColumns(session, handle)) {
            ColumnMetadata.Builder builder = ColumnMetadata.builder()
                    .setName(normalizeIdentifier(session, column.getColumnName()))
                    .setType(column.getColumnType())
                    .setNullable(column.isNullable());
            column.getComment().ifPresent(builder::setComment);
            columnMetadata.add(builder.build());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return clickHouseClient.getTableNames(session, JdbcIdentity.from(session), schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle clickHouseTableHandle = (JdbcTableHandle) tableHandle;

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (JdbcColumnHandle column : clickHouseClient.getColumns(session, clickHouseTableHandle)) {
            columnHandles.put(normalizeIdentifier(session, column.getColumnName()), column);
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
                JdbcTableHandle tableHandle = clickHouseClient.getTableHandle(session, JdbcIdentity.from(session), tableName);
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
        JdbcColumnHandle jdbcColumnHandle = (JdbcColumnHandle) columnHandle;
        ColumnMetadata.Builder builder = ColumnMetadata.builder()
                .setName(jdbcColumnHandle.getColumnName())
                .setType(jdbcColumnHandle.getColumnType())
                .setNullable(jdbcColumnHandle.isNullable());
        jdbcColumnHandle.getComment().ifPresent(builder::setComment);
        return builder.build();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this catalog");
        }
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        clickHouseClient.dropTable(JdbcIdentity.from(session), handle);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        JdbcOutputTableHandle handle = clickHouseClient.beginCreateTable(session, tableMetadata);
        setRollback(() -> clickHouseClient.rollbackCreateTable(JdbcIdentity.from(session), handle));
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
        JdbcOutputTableHandle handle = (JdbcOutputTableHandle) tableHandle;
        clickHouseClient.commitCreateTable(JdbcIdentity.from(session), handle);
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
        JdbcOutputTableHandle handle = clickHouseClient.beginInsertTable(session, getTableMetadata(session, tableHandle));
        setRollback(() -> clickHouseClient.rollbackCreateTable(JdbcIdentity.from(session), handle));
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        JdbcOutputTableHandle clickHouseInsertHandle = (JdbcOutputTableHandle) tableHandle;
        clickHouseClient.finishInsertTable(JdbcIdentity.from(session), clickHouseInsertHandle);
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        clickHouseClient.addColumn(JdbcIdentity.from(session), tableHandle, columnMetadata);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        clickHouseClient.dropColumn(JdbcIdentity.from(session), tableHandle, columnHandle);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column, String target)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        clickHouseClient.renameColumn(JdbcIdentity.from(session), tableHandle, columnHandle, target);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle table, SchemaTableName newTableName)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        clickHouseClient.renameTable(JdbcIdentity.from(session), tableHandle, newTableName);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        List<JdbcColumnHandle> columns = columnHandles.stream().map(JdbcColumnHandle.class::cast).collect(Collectors.toList());
        return clickHouseClient.getTableStatistics(session, handle, columns, constraint.getSummary());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        clickHouseClient.createSchema(JdbcIdentity.from(session), schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        clickHouseClient.dropSchema(JdbcIdentity.from(session), schemaName);
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return clickHouseConfig.isCaseSensitiveNameMatching() ? identifier : identifier.toLowerCase(ROOT);
    }
}
