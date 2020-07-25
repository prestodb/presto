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
package com.facebook.presto.connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchHandleResolver;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class MockConnectorFactory
        implements ConnectorFactory
{
    private final Function<ConnectorSession, List<String>> listSchemaNames;
    private final BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables;
    private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
    private final BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, TpchColumnHandle>> getColumnHandles;
    private final Supplier<TableStatistics> getTableStatistics;

    private MockConnectorFactory(
            Function<ConnectorSession, List<String>> listSchemaNames,
            BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
            BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
            BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, TpchColumnHandle>> getColumnHandles,
            Supplier<TableStatistics> getTableStatistics)
    {
        this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
        this.listTables = requireNonNull(listTables, "listTables is null");
        this.getViews = requireNonNull(getViews, "getViews is null");
        this.getColumnHandles = requireNonNull(getColumnHandles, "getColumnHandles is null");
        this.getTableStatistics = requireNonNull(getTableStatistics, "getTableStatistics is null");
    }

    @Override
    public String getName()
    {
        return "mock";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new TpchHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return new MockConnector(context, listSchemaNames, listTables, getViews, getColumnHandles, getTableStatistics);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private static class MockConnector
            implements Connector
    {
        private final ConnectorContext context;
        private final Function<ConnectorSession, List<String>> listSchemaNames;
        private final BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables;
        private final BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews;
        private final BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, TpchColumnHandle>> getColumnHandles;
        private final Supplier<TableStatistics> getTableStatistics;

        public MockConnector(
                ConnectorContext context,
                Function<ConnectorSession, List<String>> listSchemaNames,
                BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables,
                BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews,
                BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, TpchColumnHandle>> getColumnHandles,
                Supplier<TableStatistics> getTableStatistics)
        {
            this.context = requireNonNull(context, "context is null");
            this.listSchemaNames = requireNonNull(listSchemaNames, "listSchemaNames is null");
            this.listTables = requireNonNull(listTables, "listTables is null");
            this.getViews = requireNonNull(getViews, "getViews is null");
            this.getColumnHandles = requireNonNull(getColumnHandles, "getColumnHandles is null");
            this.getTableStatistics = requireNonNull(getTableStatistics, "getTableStatistics is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return new ConnectorTransactionHandle() {};
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
        {
            return new MockConnectorMetadata();
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TpchSplitManager(context.getNodeManager(), 1);
        }

        @Override
        public ConnectorRecordSetProvider getRecordSetProvider()
        {
            return new TpchRecordSetProvider();
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
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
            {
                return null;
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
            {
                return listTables.apply(session, schemaNameOrNull);
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
            {
                return (Map<String, ColumnHandle>) (Map) getColumnHandles.apply(session, tableHandle);
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
            {
                TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) columnHandle;
                return new ColumnMetadata(tpchColumnHandle.getColumnName(), tpchColumnHandle.getType());
            }

            @Override
            public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
            {
                return listTables(session, prefix.getSchemaName()).stream()
                        .collect(toImmutableMap(table -> table, table -> IntStream.range(0, 100)
                                .boxed()
                                .map(i -> new ColumnMetadata("column_" + i, createUnboundedVarcharType()))
                                .collect(toImmutableList())));
            }

            @Override
            public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
            {
                return ImmutableList.of();
            }

            @Override
            public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
            {
                throw new UnsupportedOperationException();
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
        }
    }

    public static final class Builder
    {
        private Function<ConnectorSession, List<String>> listSchemaNames = (session) -> ImmutableList.of();
        private BiFunction<ConnectorSession, String, List<SchemaTableName>> listTables = (session, schemaName) -> ImmutableList.of();
        private BiFunction<ConnectorSession, SchemaTablePrefix, Map<SchemaTableName, ConnectorViewDefinition>> getViews = (session, schemaTablePrefix) -> ImmutableMap.of();
        private BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, TpchColumnHandle>> getColumnHandles = (session, tableHandle) -> notSupported();
        private Supplier<TableStatistics> getTableStatistics = TableStatistics::empty;

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

        public Builder withGetColumnHandles(BiFunction<ConnectorSession, ConnectorTableHandle, Map<String, TpchColumnHandle>> getColumnHandles)
        {
            this.getColumnHandles = requireNonNull(getColumnHandles, "getColumnHandles is null");
            return this;
        }

        public Builder withGetTableStatistics(Supplier<TableStatistics> getTableStatitics)
        {
            this.getTableStatistics = requireNonNull(getTableStatitics, "getTableStatistics is null");
            return this;
        }

        public MockConnectorFactory build()
        {
            return new MockConnectorFactory(listSchemaNames, listTables, getViews, getColumnHandles, getTableStatistics);
        }

        private static <T> T notSupported()
        {
            throw new UnsupportedOperationException();
        }
    }
}
