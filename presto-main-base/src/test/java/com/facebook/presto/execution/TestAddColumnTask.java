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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.ColumnPosition;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.ColumnPosition.After;
import com.facebook.presto.sql.tree.ColumnPosition.First;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.testing.TestingWarningCollector;
import com.facebook.presto.testing.TestingWarningCollectorConfig;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestAddColumnTask
{
    private static final String CATALOG_NAME = "catalog";
    private static final String TABLE_NAME = "test_table";

    private TransactionManager transactionManager;
    private Session testSession;
    private MockMetadata metadata;
    private final TestingWarningCollector warningCollector = new TestingWarningCollector(new WarningCollectorConfig(), new TestingWarningCollectorConfig().setAddWarnings(true));

    @BeforeMethod
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        Catalog testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        ColumnPropertyManager columnPropertyManager = new ColumnPropertyManager();
        columnPropertyManager.addProperties(testCatalog.getConnectorId(), emptyList());
        testSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema("schema")
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        metadata = new MockMetadata(createTestFunctionAndTypeManager(), columnPropertyManager, testCatalog.getConnectorId());
    }

    @Test
    public void testAddColumnWithoutPositionAppends()
    {
        execute(addColumn());
        assertEquals(metadata.getReceivedPosition(), new ColumnPosition.Last());
    }

    @Test
    public void testAddColumnFirst()
    {
        execute(addColumn(new First()));
        assertEquals(metadata.getReceivedPosition(), new ColumnPosition.First());
    }

    @Test
    public void testAddColumnAfter()
    {
        execute(addColumn(new After(identifier("x"))));
        assertEquals(metadata.getReceivedPosition(), new ColumnPosition.After("x"));
    }

    @Test
    public void testAddColumnAfterIsNormalized()
    {
        // The connector receives the normalized name, so it can look the target up in its own column map
        execute(addColumn(new After(new Identifier("X", true))));
        assertEquals(metadata.getReceivedPosition(), new ColumnPosition.After("x"));

        // An unquoted identifier is normalized the same way
        execute(addColumn(new After(new Identifier("X", false))));
        assertEquals(metadata.getReceivedPosition(), new ColumnPosition.After("x"));
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Column 'missing' does not exist")
    public void testAddColumnAfterMissingColumn()
    {
        execute(addColumn(new After(identifier("missing"))));
    }

    /**
     * A hidden column is in getColumnHandles but is not part of the table's column order, so it is rejected
     * here rather than being pushed down for every connector to reject on its own. The error says the column
     * is hidden rather than that it does not exist, matching how DROP COLUMN and RENAME COLUMN report it.
     */
    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Cannot position a column after hidden column 'hidden'")
    public void testAddColumnAfterHiddenColumn()
    {
        execute(addColumn(new After(identifier("hidden"))));
    }

    private static AddColumn addColumn()
    {
        return new AddColumn(
                QualifiedName.of(TABLE_NAME),
                columnDefinition(),
                false,
                false);
    }

    private static AddColumn addColumn(com.facebook.presto.sql.tree.ColumnPosition position)
    {
        return new AddColumn(
                QualifiedName.of(TABLE_NAME),
                columnDefinition(),
                Optional.of(position),
                false,
                false);
    }

    private static ColumnDefinition columnDefinition()
    {
        return new ColumnDefinition(identifier("c"), "BIGINT", true, emptyList(), Optional.empty());
    }

    private void execute(AddColumn statement)
    {
        getFutureValue(new AddColumnTask().execute(statement, transactionManager, metadata, new AllowAllAccessControl(), testSession, emptyList(), warningCollector, ""));
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final ColumnPropertyManager columnPropertyManager;
        private final ConnectorId catalogHandle;
        private final TableHandle tableHandle;
        private ColumnPosition receivedPosition;

        public MockMetadata(FunctionAndTypeManager functionAndTypeManager, ColumnPropertyManager columnPropertyManager, ConnectorId catalogHandle)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.tableHandle = new TableHandle(catalogHandle, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty());
        }

        @Override
        public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
        {
            this.receivedPosition = position;
        }

        public ColumnPosition getReceivedPosition()
        {
            return receivedPosition;
        }

        @Override
        public MetadataResolver getMetadataResolver(Session session)
        {
            return new MetadataResolver()
            {
                @Override
                public boolean catalogExists(String catalogName)
                {
                    return catalogHandle.getCatalogName().equals(catalogName);
                }

                @Override
                public boolean schemaExists(CatalogSchemaName schemaName)
                {
                    return true;
                }

                @Override
                public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
                {
                    return Optional.of(tableHandle);
                }

                @Override
                public List<ColumnMetadata> getColumns(TableHandle tableHandle)
                {
                    return emptyList();
                }

                @Override
                public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
                {
                    return ImmutableMap.of();
                }

                @Override
                public Optional<ViewDefinition> getView(QualifiedObjectName viewName)
                {
                    return Optional.empty();
                }

                @Override
                public Optional<MaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName)
                {
                    return Optional.empty();
                }
            };
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            // "x" is the only real column, so AFTER x resolves and AFTER missing does not. "hidden" stands for a
            // synthesized column, which a connector exposes here even though it is not part of the column order
            return ImmutableMap.of(
                    "x", new TestingColumnHandle("x"),
                    "hidden", new TestingColumnHandle("hidden"));
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            String columnName = ((TestingColumnHandle) columnHandle).getName();
            return ColumnMetadata.builder()
                    .setName(columnName)
                    .setType(BIGINT)
                    .setHidden(columnName.equals("hidden"))
                    .build();
        }

        @Override
        public ColumnPropertyManager getColumnPropertyManager()
        {
            return columnPropertyManager;
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return functionAndTypeManager.getType(signature);
        }

        @Override
        public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
        {
            if (catalogHandle.getCatalogName().equals(catalogName)) {
                return Optional.of(catalogHandle);
            }
            return Optional.empty();
        }
    }
}
