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
package com.facebook.presto.connector.system;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.table.ExcludeColumns;
import com.facebook.presto.operator.table.Sequence;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.TableFunctionProcessorProvider;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.transaction.InternalConnector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class GlobalSystemConnector
        implements InternalConnector
{
    public static final String NAME = "system";

    private final String connectorId;
    private final Set<SystemTable> systemTables;
    private final Set<Procedure> procedures;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final NodeManager nodeManager;
    private final FunctionAndTypeManager functionAndTypeManager;

    public GlobalSystemConnector(String connectorId, Set<SystemTable> systemTables, Set<Procedure> procedures, Set<ConnectorTableFunction> tableFunctions, NodeManager nodeManager, FunctionAndTypeManager functionAndTypeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.systemTables = ImmutableSet.copyOf(requireNonNull(systemTables, "systemTables is null"));
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.tableFunctions = ImmutableSet.copyOf(requireNonNull(tableFunctions, "tableFunctions is null"));
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new GlobalSystemTransactionHandle(connectorId, transactionId);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return new ConnectorMetadata()
        {
            @Override
            public List<String> listSchemaNames(ConnectorSession session)
            {
                return ImmutableList.of();
            }

            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
            {
                return null;
            }

            @Override
            public ConnectorTableLayoutResult getTableLayoutForConstraint(
                    ConnectorSession session,
                    ConnectorTableHandle table,
                    Constraint<ColumnHandle> constraint,
                    Optional<Set<ColumnHandle>> desiredColumns)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
            {
                return ImmutableList.of();
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
            {
                return ImmutableMap.of();
            }
        };
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new ConnectorSplitManager() {
            @Override
            public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function)
            {
                return function.getSplits(transaction, session, nodeManager, functionAndTypeManager);
            }
        };
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return new ConnectorPageSourceProvider() {
            @Override
            public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, ConnectorTableLayoutHandle layout, List<ColumnHandle> columns, SplitContext splitContext, RuntimeStats runtimeStats)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    @Override
    public Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> getTableFunctionProcessorProvider()
    {
        return connectorTableFunctionHandle -> {
            if (connectorTableFunctionHandle instanceof ExcludeColumns.ExcludeColumnsFunctionHandle) {
                return ExcludeColumns.getExcludeColumnsFunctionProcessorProvider();
            }
            else if (connectorTableFunctionHandle instanceof Sequence.SequenceFunctionHandle) {
                return Sequence.getSequenceFunctionProcessorProvider();
            }
            return null;
        };
    }
}
