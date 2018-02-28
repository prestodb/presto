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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.NestedColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingPageSinkProvider;
import com.facebook.presto.testing.TestingSplitManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.RowType.field;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class TestMergeNestedColumns
        extends BasePlanTest
{
    private static final Type MSG_TYPE = RowType.from(ImmutableList.of(field("x", VarcharType.VARCHAR), field("y", VarcharType.VARCHAR)));

    public TestMergeNestedColumns()
    {
        super(TestMergeNestedColumns::createQueryRunner);
    }

    @Test
    public void testSelectDereference()
    {
        assertPlan("select foo.x, foo.y, bar.x, bar.y from nested_column_table",
                output(ImmutableList.of("foo_x", "foo_y", "bar_x", "bar_y"),
                        project(ImmutableMap.of("foo_x", expression("foo_x"), "foo_y", expression("foo_y"), "bar_x", expression("bar.x"), "bar_y", expression("bar.y")),
                                tableScan("nested_column_table", ImmutableMap.of("foo_x", "foo.x", "foo_y", "foo.y", "bar", "bar")))));
    }

    @Test
    public void testSelectDereferenceAndParentDoesNotFire()
    {
        assertPlan("select foo.x, foo.y, foo from nested_column_table",
                output(ImmutableList.of("foo_x", "foo_y", "foo"),
                        project(ImmutableMap.of("foo_x", expression("foo.x"), "foo_y", expression("foo.y"), "foo", expression("foo")),
                                tableScan("nested_column_table", ImmutableMap.of("foo", "foo")))));
    }

    private static LocalQueryRunner createQueryRunner()
    {
        String schemaName = "test-schema";
        String catalogName = "test";
        TableInfo regularTable = new TableInfo(new SchemaTableName(schemaName, "regular_table"),
                ImmutableList.of(new ColumnMetadata("dummy_column", VarcharType.VARCHAR)), ImmutableMap.of());

        TableInfo nestedColumnTable = new TableInfo(new SchemaTableName(schemaName, "nested_column_table"),
                ImmutableList.of(new ColumnMetadata("foo", MSG_TYPE), new ColumnMetadata("bar", MSG_TYPE)), ImmutableMap.of(
                new NestedColumn(ImmutableList.of("foo", "x")), 0,
                new NestedColumn(ImmutableList.of("foo", "y")), 0));

        ImmutableList<TableInfo> tableInfos = ImmutableList.of(regularTable, nestedColumnTable);

        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema(schemaName)
                .build());
        queryRunner.createCatalog(catalogName, new TestConnectorFactory(new TestMetadata(tableInfos)), ImmutableMap.of());
        return queryRunner;
    }

    private static class TestConnectorFactory
            implements ConnectorFactory
    {
        private final TestMetadata metadata;

        public TestConnectorFactory(TestMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new TestingHandleResolver();
        }

        @Override
        public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
        {
            return new TestConnector(metadata);
        }
    }

    private enum TransactionInstance
            implements ConnectorTransactionHandle
    {
        INSTANCE
    }

    private static class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        private TestConnector(ConnectorMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return TransactionInstance.INSTANCE;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TestingSplitManager(ImmutableList.of());
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return (transactionHandle, session, split, columns) -> new FixedPageSource(ImmutableList.of());
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return new TestingPageSinkProvider();
        }
    }

    private static class TestMetadata
            extends TestingMetadata
    {
        private final List<TableInfo> tableInfos;

        TestMetadata(List<TableInfo> tableInfos)
        {
            this.tableInfos = requireNonNull(tableInfos, "tableinfos is null");
            insertTables();
        }

        private void insertTables()
        {
            for (TableInfo tableInfo : tableInfos) {
                getTables().put(tableInfo.getSchemaTableName(), tableInfo.getTableMetadata());
            }
        }

        @Override
        public Map<NestedColumn, ColumnHandle> getNestedColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<NestedColumn> dereferences)
        {
            requireNonNull(tableHandle, "tableHandle is null");
            SchemaTableName tableName = getTableName(tableHandle);
            return tableInfos.stream().filter(tableInfo -> tableInfo.getSchemaTableName().equals(tableName)).map(TableInfo::getNestedColumnHandle).findFirst().orElse(ImmutableMap.of());
        }

        @Override
        public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
        {
            return new ConnectorTableLayout(new ConnectorTableLayoutHandle() {});
        }

        @Override
        public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
        {
            return ImmutableList.of(new ConnectorTableLayoutResult(new ConnectorTableLayout(new ConnectorTableLayoutHandle() {}), constraint.getSummary()));
        }
    }

    private static class TableInfo
    {
        private final SchemaTableName schemaTableName;
        private final List<ColumnMetadata> columnMetadatas;
        private final Map<NestedColumn, Integer> nestedColumns;

        public TableInfo(SchemaTableName schemaTableName, List<ColumnMetadata> columnMetadata, Map<NestedColumn, Integer> nestedColumns)
        {
            this.schemaTableName = schemaTableName;
            this.columnMetadatas = columnMetadata;
            this.nestedColumns = nestedColumns;
        }

        SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        ConnectorTableMetadata getTableMetadata()
        {
            return new ConnectorTableMetadata(schemaTableName, columnMetadatas);
        }

        Map<NestedColumn, ColumnHandle> getNestedColumnHandle()
        {
            return nestedColumns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                Preconditions.checkArgument(entry.getValue() >= 0 && entry.getValue() < columnMetadatas.size(), "index is not valid");
                NestedColumn nestedColumn = entry.getKey();
                return new TestingMetadata.TestingColumnHandle(nestedColumn.getName(), entry.getValue(), VarcharType.VARCHAR);
            }));
        }
    }
}
