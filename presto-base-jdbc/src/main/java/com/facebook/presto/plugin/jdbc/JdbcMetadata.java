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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.plugin.jdbc.optimization.JdbcJoinPredicateToSqlTranslator;
import com.facebook.presto.plugin.jdbc.optimization.function.JoinOperatorTranslators;
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
import com.facebook.presto.spi.JoinTableInfo;
import com.facebook.presto.spi.JoinTableSet;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JdbcMetadata
        implements ConnectorMetadata
{
    private final JdbcMetadataCache jdbcMetadataCache;
    private final JdbcClient jdbcClient;
    private final boolean allowDropTable;
    private final String url;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();
    private final FunctionMetadataManager functionMetadataManager;

    public JdbcMetadata(JdbcMetadataCache jdbcMetadataCache, JdbcClient jdbcClient, boolean allowDropTable, TableLocationProvider tableLocationProvider, FunctionMetadataManager functionMetadataManager)
    {
        this.jdbcMetadataCache = requireNonNull(jdbcMetadataCache, "jdbcMetadataCache is null");
        this.jdbcClient = requireNonNull(jdbcClient, "client is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.allowDropTable = allowDropTable;
        this.url = requireNonNull(tableLocationProvider, "tableLocationProvider is null").getTableLocation();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return jdbcClient.schemaExists(session, JdbcIdentity.from(session), schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(jdbcClient.getSchemaNames(session, JdbcIdentity.from(session)));
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return jdbcMetadataCache.getTableHandle(session, tableName);
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new JdbcTableLayoutHandle(session.getSqlFunctionProperties(), tableHandle, constraint.getSummary(), Optional.empty()));
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
        for (JdbcColumnHandle column : jdbcMetadataCache.getColumns(session, handle)) {
            columnMetadata.add(column.getColumnMetadata(session, jdbcClient));
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return jdbcClient.getTableNames(session, JdbcIdentity.from(session), schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableHandle;

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (JdbcColumnHandle column : jdbcMetadataCache.getColumns(session, jdbcTableHandle)) {
            columnHandles.put(column.getColumnMetadata(session, jdbcClient).getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables;
        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            tables = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tables = listTables(session, Optional.ofNullable(prefix.getSchemaName()));
        }
        for (SchemaTableName tableName : tables) {
            try {
                JdbcTableHandle tableHandle = jdbcMetadataCache.getTableHandle(session, tableName);
                if (tableHandle == null) {
                    continue;
                }
                columns.put(tableName, getTableMetadata(session, tableHandle).getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((JdbcColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this catalog");
        }
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        jdbcClient.dropTable(session, JdbcIdentity.from(session), handle);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        JdbcOutputTableHandle handle = jdbcClient.beginCreateTable(session, tableMetadata);
        setRollback(() -> jdbcClient.rollbackCreateTable(session, JdbcIdentity.from(session), handle));
        return handle;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        jdbcClient.createTable(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        JdbcOutputTableHandle handle = (JdbcOutputTableHandle) tableHandle;
        jdbcClient.commitCreateTable(session, JdbcIdentity.from(session), handle);
        clearRollback();
        return Optional.of(new JdbcOutputMetadata(url));
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
        JdbcOutputTableHandle handle = jdbcClient.beginInsertTable(session, getTableMetadata(session, tableHandle));
        setRollback(() -> jdbcClient.rollbackCreateTable(session, JdbcIdentity.from(session), handle));
        return handle;
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        jdbcClient.truncateTable(session, JdbcIdentity.from(session), handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        JdbcOutputTableHandle jdbcInsertHandle = (JdbcOutputTableHandle) tableHandle;
        jdbcClient.finishInsertTable(session, JdbcIdentity.from(session), jdbcInsertHandle);
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        jdbcClient.addColumn(session, JdbcIdentity.from(session), tableHandle, columnMetadata);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        jdbcClient.dropColumn(session, JdbcIdentity.from(session), tableHandle, columnHandle);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column, String target)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        jdbcClient.renameColumn(session, JdbcIdentity.from(session), tableHandle, columnHandle, target);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle table, SchemaTableName newTableName)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        jdbcClient.renameTable(session, JdbcIdentity.from(session), tableHandle, newTableName);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        List<JdbcColumnHandle> columns = columnHandles.stream().map(JdbcColumnHandle.class::cast).collect(Collectors.toList());
        return jdbcClient.getTableStatistics(session, handle, columns, constraint.getSummary());
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return jdbcClient.normalizeIdentifier(session, identifier);
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle tableHandle)
    {
        return Optional.of(new JdbcInputInfo(url));
    }

    public boolean isPushdownSupportedForFilter(ConnectorSession session,
            ConnectorTableHandle tableHandle,
            RowExpression filter,
            Map<VariableReferenceExpression, ColumnHandle> symbolToColumnHandleMap)
    {
        if (tableHandle instanceof JoinTableSet) {
            JdbcJoinPredicateToSqlTranslator jdbcJoinPredicateToSqlTranslator = new JdbcJoinPredicateToSqlTranslator(
                    functionMetadataManager,
                    buildFunctionTranslator(ImmutableSet.of(JoinOperatorTranslators.class)), "");
            TranslatedExpression<JdbcExpression> jdbcExpression = translateWith(
                    filter,
                    jdbcJoinPredicateToSqlTranslator,
                    symbolToColumnHandleMap);
            return jdbcExpression.getTranslated().isPresent();
        }
        return false;
    }

    @Override
    public TableScanNode buildJoinTableScanNode(TableScanNode updatedTableScanNode, TableHandle intermediateTableHandle)
    {
        JoinTableSet joinTableSet = (JoinTableSet) intermediateTableHandle.getConnectorHandle();
        ImmutableList.Builder<JoinTableInfo> joinTableInfoBuilder = ImmutableList.builder();
        generateAllJoinTableInfos(joinTableSet, joinTableInfoBuilder);
        ImmutableList.Builder<ConnectorTableHandle> newConnectorTableHandlesBuilder = ImmutableList.builder();
        Map<VariableReferenceExpression, ColumnHandle> groupAssignments = new HashMap<>();

        //Generate aliases for each table being joined to avoid any ambiguous column name references.
        // These aliases are used in QueryBuilder#buildSql
        final String aliasPrefix = "T";
        int aliasTableCounter = 0;

        // Create new table handles and update column handles
        for (JoinTableInfo tableMapping : joinTableInfoBuilder.build()) {
            JdbcTableHandle handle = (JdbcTableHandle) tableMapping.getTableHandle();
            JdbcTableHandle newHandle = new JdbcTableHandle(handle.getConnectorId(), handle.getSchemaTableName(), handle.getCatalogName(),
                    handle.getSchemaName(), handle.getTableName(), handle.getJoinTables(), Optional.of(aliasPrefix + (++aliasTableCounter)));
            newConnectorTableHandlesBuilder.add(newHandle);

            tableMapping.getAssignments().forEach((key, oldColumnHandle) -> {
                JdbcColumnHandle newColumnHandle = new JdbcColumnHandle(((JdbcColumnHandle) oldColumnHandle).getConnectorId(), ((JdbcColumnHandle) oldColumnHandle).getColumnName(),
                        ((JdbcColumnHandle) oldColumnHandle).getJdbcTypeHandle(), ((JdbcColumnHandle) oldColumnHandle).getColumnType(), ((JdbcColumnHandle) oldColumnHandle).isNullable(),
                        ((JdbcColumnHandle) oldColumnHandle).getComment(), newHandle.getTableAlias());
                groupAssignments.put(key, newColumnHandle); // Update the map entry
            });
        }

        List<ConnectorTableHandle> newConnectorTableHandles = newConnectorTableHandlesBuilder.build();
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) newConnectorTableHandles.get(0);
        JdbcTableHandle newConnectorTableHandle = new JdbcTableHandle(jdbcTableHandle.getConnectorId(), jdbcTableHandle.getSchemaTableName(), jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName(), newConnectorTableHandles, jdbcTableHandle.getTableAlias());

        Optional<ConnectorTableLayoutHandle> optionalTableLayoutHandle = intermediateTableHandle.getLayout().map(oldTableLayoutHandle -> {
            JdbcTableLayoutHandle oldLayout = (JdbcTableLayoutHandle) oldTableLayoutHandle;
            return new JdbcTableLayoutHandle(newConnectorTableHandle, oldLayout.getTupleDomain(), oldLayout.getAdditionalPredicate(), oldLayout.getLayoutString());
        });

        TableHandle newTableHandle = new TableHandle(intermediateTableHandle.getConnectorId(), newConnectorTableHandle, intermediateTableHandle.getTransaction(), optionalTableLayoutHandle, intermediateTableHandle.getDynamicFilter());
        return new TableScanNode(updatedTableScanNode.getSourceLocation(), updatedTableScanNode.getId(), newTableHandle, updatedTableScanNode.getOutputVariables(), groupAssignments, updatedTableScanNode.getCurrentConstraint(), updatedTableScanNode.getEnforcedConstraint(), updatedTableScanNode.getCteMaterializationInfo());
    }

    /**
     * Recursively build all join table infos from a JoinTableSet
     *
     * @param tableHandles
     * @param joinTableInfoBuilder
     * @return
     */
    private void generateAllJoinTableInfos(JoinTableSet tableHandles, ImmutableList.Builder<JoinTableInfo> joinTableInfoBuilder)
    {
        for (JoinTableInfo innerJoinTableInfo : tableHandles.getInnerJoinTableInfos()) {
            if (innerJoinTableInfo.getTableHandle() instanceof JoinTableSet) {
                generateAllJoinTableInfos((JoinTableSet) innerJoinTableInfo.getTableHandle(), joinTableInfoBuilder);
            }
            else {
                joinTableInfoBuilder.add(innerJoinTableInfo);
            }
        }
    }
}
