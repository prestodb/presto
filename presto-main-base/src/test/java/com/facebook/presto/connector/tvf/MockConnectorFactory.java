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
package com.facebook.presto.connector.tvf;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.TableFunctionSplitResolver;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.TableFunctionProcessorProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.util.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class MockConnectorFactory
        implements ConnectorFactory
{
    private final Function<ConnectorSession, List<String>> listSchemaNames;
    private final BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
    private final BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, MockConnectorColumnHandle>> getColumnHandles;
    private final Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> tableFunctionProcessorProvider;
    private final MockTableFunctionHandleResolver tableFunctionHandleResolver;
    private final MockTableFunctionSplitResolver tableFunctionSplitResolver;
    private final Supplier<TableStatistics> getTableStatistics;
    private final ApplyTableFunction applyTableFunction;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final Function<ConnectorTableFunctionHandle, ConnectorSplitSource> tableFunctionSplitsSources;

    public MockConnectorFactory(
            Function<ConnectorSession, List<String>> listSchemaNames,
            BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
            BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, MockConnectorColumnHandle>> getColumnHandles,
            Supplier<TableStatistics> getTableStatistics,
            ApplyTableFunction applyTableFunction,
            Set<ConnectorTableFunction> tableFunctions,
            MockTableFunctionHandleResolver tableFunctionHandleResolver,
            MockTableFunctionSplitResolver tableFunctionSplitResolver,
            Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> tableFunctionProcessorProvider,
            Function<ConnectorTableFunctionHandle, ConnectorSplitSource> tableFunctionSplitsSources)
    {
        this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
        this.listTables = requireNonNull(listTables, "listTables is null");
        this.getViews = requireNonNull(getViews, "getViews is null");
        this.getColumnHandles = requireNonNull(getColumnHandles, "getColumnHandles is null");
        this.getTableStatistics = requireNonNull(getTableStatistics, "getTableStatistics is null");
        this.applyTableFunction = requireNonNull(applyTableFunction, "applyTableFunction is null");
        this.tableFunctions = requireNonNull(tableFunctions, "tableFunctions is null");
        this.tableFunctionHandleResolver = requireNonNull(tableFunctionHandleResolver, "tableFunctionHandleResolver is null");
        this.tableFunctionSplitResolver = requireNonNull(tableFunctionSplitResolver, "tableFunctionSplitResolver is null");
        this.tableFunctionProcessorProvider = requireNonNull(tableFunctionProcessorProvider, "tableFunctionProcessorProvider is null");
        this.tableFunctionSplitsSources = requireNonNull(tableFunctionSplitsSources, "tableFunctionSplitsSources is null");
    }

    @Override
    public String getName()
    {
        return "mock";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new MockHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return new MockConnector(context, listSchemaNames, listTables, getViews, getColumnHandles, getTableStatistics, applyTableFunction, tableFunctions, tableFunctionProcessorProvider, tableFunctionSplitsSources);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Function<SchemaTableName, List<ColumnMetadata>> defaultGetColumns()
    {
        return table -> IntStream.range(0, 100)
                .boxed()
                .map(i -> ColumnMetadata.builder().setName("column_" + i).setType(createUnboundedVarcharType()).build())
                .collect(toImmutableList());
    }

    @Override
    public Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> getTableFunctionProcessorProvider()
    {
        return tableFunctionProcessorProvider;
    }

    @Override
    public Optional<TableFunctionHandleResolver> getTableFunctionHandleResolver()
    {
        return Optional.of(tableFunctionHandleResolver);
    }

    @Override
    public Optional<TableFunctionSplitResolver> getTableFunctionSplitResolver()
    {
        return Optional.of(tableFunctionSplitResolver);
    }

    @FunctionalInterface
    public interface ApplyTableFunction
    {
        Optional<TableFunctionApplicationResult<ConnectorTableHandle>> apply(ConnectorSession session, ConnectorTableFunctionHandle handle);
    }

    public static class MockConnector
            implements Connector
    {
        private static final String DELETE_ROW_ID = "delete_row_id";
        private static final String UPDATE_ROW_ID = "update_row_id";
        private static final String MERGE_ROW_ID = "merge_row_id";

        private final ConnectorContext context;
        private final Function<ConnectorSession, List<String>> listSchemaNames;
        private final BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables;
        private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
        private final BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, MockConnectorColumnHandle>> getColumnHandles;
        private final Supplier<TableStatistics> getTableStatistics;
        private final ApplyTableFunction applyTableFunction;
        private final Set<ConnectorTableFunction> tableFunctions;
        private final Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> tableFunctionProcessorProvider;
        private final Function<ConnectorTableFunctionHandle, ConnectorSplitSource> tableFunctionSplitsSources;

        public MockConnector(
                ConnectorContext context,
                Function<ConnectorSession, List<String>> listSchemaNames,
                BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
                BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
                BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, MockConnectorColumnHandle>> getColumnHandles,
                Supplier<TableStatistics> getTableStatistics,
                ApplyTableFunction applyTableFunction,
                Set<ConnectorTableFunction> tableFunctions,
                Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> getTableFunctionProcessorProvider,
                Function<ConnectorTableFunctionHandle, ConnectorSplitSource> tableFunctionSplitsSources)
        {
            this.context = requireNonNull(context, "context is null");
            this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
            this.listTables = requireNonNull(listTables, "listTables is null");
            this.getViews = requireNonNull(getViews, "getViews is null");
            this.getColumnHandles = requireNonNull(getColumnHandles, "getColumnHandles is null");
            this.getTableStatistics = requireNonNull(getTableStatistics, "getTableStatistics is null");
            this.applyTableFunction = requireNonNull(applyTableFunction, "applyTableFunction is null");
            this.tableFunctions = requireNonNull(tableFunctions, "tableFunctions is null");
            this.tableFunctionProcessorProvider = requireNonNull(getTableFunctionProcessorProvider, "tableFunctionProcessorProvider is null");
            this.tableFunctionSplitsSources = requireNonNull(tableFunctionSplitsSources, "tableFunctionSplitsSources is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return MockConnectorTransactionHandle.INSTANCE;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
        {
            return new MockConnectorMetadata();
        }

        public enum MockConnectorSplit
                implements ConnectorSplit
        {
            MOCK_CONNECTOR_SPLIT;

            @Override
            public NodeSelectionStrategy getNodeSelectionStrategy()
            {
                return NO_PREFERENCE;
            }

            @Override
            public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
            {
                return Collections.emptyList();
            }

            @Override
            public Object getInfo()
            {
                return null;
            }
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new ConnectorSplitManager()
            {
                @Override
                public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
                {
                    return new FixedSplitSource(Collections.singleton(MockConnectorSplit.MOCK_CONNECTOR_SPLIT));
                }

                @Override
                public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle functionHandle)
                {
                    ConnectorSplitSource splits = tableFunctionSplitsSources.apply(functionHandle);
                    return requireNonNull(splits, "missing ConnectorSplitSource for table function handle " +
                            functionHandle.getClass().getSimpleName());
                }
            };
        }

        @Override
        public ConnectorRecordSetProvider getRecordSetProvider()
        {
            return new TpchRecordSetProvider();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return new MockConnectorPageSourceProvider();
        }

        @Override
        public Set<ConnectorTableFunction> getTableFunctions()
        {
            return tableFunctions;
        }

        @Override
        public Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> getTableFunctionProcessorProvider()
        {
            return tableFunctionProcessorProvider;
        }

        private class MockConnectorMetadata
                implements ConnectorMetadata
        {
            @Override
            public List<String> listSchemaNames(ConnectorSession session)
            {
                return listSchemaNames.apply(session);
            }

            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
            {
                return new ConnectorTableHandle() {};
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                MockConnectorTableHandle table = (MockConnectorTableHandle) tableHandle;
                return new ConnectorTableMetadata(
                        table.getTableName(),
                        defaultGetColumns().apply(table.getTableName()),
                        ImmutableMap.of());
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
            {
                return listTables.apply(session, schemaNameOrNull);
            }

            public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> properties)
            {
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                return (Map<String, ColumnHandle>) (Map) getColumnHandles.apply(session, tableHandle);
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
            {
                if (columnHandle instanceof MockConnectorColumnHandle) {
                    MockConnectorColumnHandle mockColumnHandle = (MockConnectorColumnHandle) columnHandle;
                    return ColumnMetadata.builder().setName(mockColumnHandle.getName()).setType(mockColumnHandle.getType()).build();
                }
                else {
                    TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) columnHandle;
                    return ColumnMetadata.builder().setName(tpchColumnHandle.getColumnName()).setType(tpchColumnHandle.getType()).build();
                }
            }

            @Override
            public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
            {
                return listTables(session, prefix.getSchemaName()).stream()
                        .collect(toImmutableMap(table -> table, table -> IntStream.range(0, 100)
                                .boxed()
                                .map(i -> ColumnMetadata.builder().setName("column_" + i).setType(createUnboundedVarcharType()).build())
                                .collect(toImmutableList())));
            }

            @Override
            public ConnectorTableLayoutResult getTableLayoutForConstraint(
                    ConnectorSession session,
                    ConnectorTableHandle table,
                    Constraint<ColumnHandle> constraint,
                    Optional<Set<ColumnHandle>> desiredColumns)
            {
                // TODO: Currently not supporting constraints
                MockTableLayoutHandle mock = new MockTableLayoutHandle((MockConnectorTableHandle) table, TupleDomain.none());
                return new ConnectorTableLayoutResult(new ConnectorTableLayout(mock,
                        Optional.empty(),
                        mock.getPredicate(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Collections.emptyList(),
                        Optional.empty()), TupleDomain.none());
            }

            @Override
            public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
            {
                MockTableLayoutHandle mock = (MockTableLayoutHandle) handle;
                return new ConnectorTableLayout(handle);
            }

            @Override
            public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
            {
                return getViews.apply(session, prefix);
            }

            @Override
            public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
            {
                return getTableStatistics.get();
            }

            @Override
            public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
            {
                return applyTableFunction.apply(session, handle);
            }
        }

        private class MockConnectorPageSourceProvider
                implements ConnectorPageSourceProvider
        {
            @Override
            //public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, SplitContext splitContext)
            public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, ConnectorTableLayoutHandle layout, List<ColumnHandle> columns, SplitContext splitContext, RuntimeStats runtimeStats)
            {
                MockConnectorTableHandle handle = ((MockTableLayoutHandle) layout).getTable();
                SchemaTableName tableName = handle.getTableName();
                List<MockConnectorColumnHandle> projection = columns.stream()
                        .map(MockConnectorColumnHandle.class::cast)
                        .collect(toImmutableList());
                List<Type> types = columns.stream()
                        .map(MockConnectorColumnHandle.class::cast)
                        .map(MockConnectorColumnHandle::getType)
                        .collect(toImmutableList());
                Map<String, Integer> columnIndexes = getColumnIndexes(tableName);
                /*
                List<List<?>> records = data.apply(tableName).stream()
                        .map(record -> {
                            ImmutableList.Builder<Object> projectedRow = ImmutableList.builder();
                            for (MockConnectorColumnHandle column : projection) {
                                String columnName = column.getName();
                                if (columnName.equals(DELETE_ROW_ID) || columnName.equals(UPDATE_ROW_ID) || columnName.equals(MERGE_ROW_ID)) {
                                    projectedRow.add(0);
                                    continue;
                                }
                                Integer index = columnIndexes.get(columnName);
                                requireNonNull(index, "index is null");
                                projectedRow.add(record.get(index));
                            }
                            return projectedRow.build();
                        })
                        .collect(toImmutableList());*/

                return new MockConnectorPageSource(new RecordPageSource(new InMemoryRecordSet(types, ImmutableList.of())));
            }

            private Map<String, Integer> getColumnIndexes(SchemaTableName tableName)
            {
                ImmutableMap.Builder<String, Integer> columnIndexes = ImmutableMap.builder();
                List<ColumnMetadata> columnMetadata = defaultGetColumns().apply(tableName);
                for (int index = 0; index < columnMetadata.size(); index++) {
                    columnIndexes.put(columnMetadata.get(index).getName(), index);
                }
                return columnIndexes.buildOrThrow();
            }
        }
    }

    public static class MockTableFunctionHandleResolver
            implements TableFunctionHandleResolver
    {
        Set<Class<? extends ConnectorTableFunctionHandle>> handles = Sets.newHashSet();

        @Override
        public Set<Class<? extends ConnectorTableFunctionHandle>> getTableFunctionHandleClasses()
        {
            return handles;
        }

        public void addTableFunctionHandle(Class<? extends ConnectorTableFunctionHandle> tableFunctionHandleClass)
        {
            handles.add(tableFunctionHandleClass);
        }
    }

    public static class MockTableFunctionSplitResolver
            implements TableFunctionSplitResolver
    {
        Set<Class<? extends ConnectorSplit>> handles = Sets.newHashSet();

        @Override
        public Set<Class<? extends ConnectorSplit>> getTableFunctionSplitClasses()
        {
            return handles;
        }

        public void addSplitClass(Class<? extends ConnectorSplit> splitClass)
        {
            handles.add(splitClass);
        }
    }

    public static final class Builder
    {
        private Function<ConnectorSession, List<String>> listSchemaNames = (session) -> ImmutableList.of();
        private BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables = (session, schemaName) -> ImmutableList.of();
        private BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews = (session, schemaTablePrefix) -> ImmutableMap.of();
        private BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, MockConnectorColumnHandle>> getColumnHandles = (session, tableHandle) -> {
            MockConnectorTableHandle table = (MockConnectorTableHandle) tableHandle;
            return defaultGetColumns().apply(table.getTableName()).stream()
                    .collect(toImmutableMap(ColumnMetadata::getName, column ->
                            new MockConnectorColumnHandle(column.getName(), column.getType())));
        };
        private Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> tableFunctionProcessorProvider = handle -> null;
        private Supplier<TableStatistics> getTableStatistics = TableStatistics::empty;
        private ApplyTableFunction applyTableFunction = (session, handle) -> Optional.empty();
        private Set<ConnectorTableFunction> tableFunctions = ImmutableSet.of();
        private MockTableFunctionHandleResolver tableFunctionHandleResolver = new MockTableFunctionHandleResolver();
        private MockTableFunctionSplitResolver tableFunctionSplitResolver = new MockTableFunctionSplitResolver();
        private Function<ConnectorTableFunctionHandle, ConnectorSplitSource> tableFunctionSplitsSources = handle -> null;

        public Builder withListSchemaNames(Function<ConnectorSession, List<String>> listSchemaNames)
        {
            this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
            return this;
        }

        public Builder withListTables(BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables)
        {
            this.listTables = requireNonNull(listTables, "listTables is null");
            return this;
        }

        public Builder withGetViews(BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews)
        {
            this.getViews = requireNonNull(getViews, "getViews is null");
            return this;
        }

        public Builder withGetColumnHandles(BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, MockConnectorColumnHandle>> getColumnHandles)
        {
            this.getColumnHandles = requireNonNull(getColumnHandles, "getColumnHandles is null");
            return this;
        }

        public Builder withGetTableStatistics(Supplier<TableStatistics> getTableStatistics)
        {
            this.getTableStatistics = requireNonNull(getTableStatistics, "getTableStatistics is null");
            return this;
        }

        public Builder withApplyTableFunction(ApplyTableFunction applyTableFunction)
        {
            this.applyTableFunction = applyTableFunction;
            return this;
        }

        public Builder withTableFunctions(Iterable<ConnectorTableFunction> tableFunctions)
        {
            this.tableFunctions = ImmutableSet.copyOf(tableFunctions);
            return this;
        }

        public Builder withTableFunctionResolver(Class<? extends ConnectorTableFunctionHandle> tableFunctionHandleclass)
        {
            this.tableFunctionHandleResolver.addTableFunctionHandle(tableFunctionHandleclass);
            return this;
        }

        public Builder withTableFunctionSplitResolver(Class<? extends ConnectorSplit> splitClass)
        {
            this.tableFunctionSplitResolver.addSplitClass(splitClass);
            return this;
        }

        public Builder withTableFunctionProcessorProvider(Function<ConnectorTableFunctionHandle, TableFunctionProcessorProvider> tableFunctionProcessorProvider)
        {
            this.tableFunctionProcessorProvider = tableFunctionProcessorProvider;
            return this;
        }

        public Builder withTableFunctionSplitSource(Function<ConnectorTableFunctionHandle, ConnectorSplitSource> sourceProvider)
        {
            tableFunctionSplitsSources = requireNonNull(sourceProvider, "sourceProvider is null");
            return this;
        }

        public MockConnectorFactory build()
        {
            return new MockConnectorFactory(listSchemaNames, listTables, getViews, getColumnHandles, getTableStatistics, applyTableFunction, tableFunctions, tableFunctionHandleResolver, tableFunctionSplitResolver, tableFunctionProcessorProvider, tableFunctionSplitsSources);
        }

        private static <T> T notSupported()
        {
            throw new UnsupportedOperationException();
        }
    }
}
