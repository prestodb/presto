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
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
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
import com.facebook.presto.sql.tree.ColumnPosition.After;
import com.facebook.presto.sql.tree.ColumnPosition.First;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetColumnPosition;
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
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestSetColumnPositionTask
{
    private static final String CATALOG_NAME = "catalog";
    private static final String TABLE_NAME = "test_table";
    private static final String HIDDEN_COLUMN_NAME = "$path";

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
        testSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema("schema")
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        metadata = new MockMetadata(testCatalog.getConnectorId(), true);
    }

    @Test
    public void testMoveColumnFirst()
    {
        execute(setColumnPosition("a", new First()));
        assertEquals(metadata.getReceivedPosition(), new ColumnPosition.First());
        assertEquals(metadata.getReceivedColumnName(), "a");
    }

    @Test
    public void testMoveColumnAfter()
    {
        execute(setColumnPosition("a", new After(identifier("b"))));
        assertEquals(metadata.getReceivedPosition(), new ColumnPosition.After("b"));
        assertEquals(metadata.getReceivedColumnName(), "a");
    }

    @Test
    public void testNamesAreNormalized()
    {
        // The connector receives normalized names, so it can look both columns up in its own column map
        execute(setColumnPosition(new Identifier("A", true), new After(new Identifier("B", true))));
        assertEquals(metadata.getReceivedPosition(), new ColumnPosition.After("b"));
        assertEquals(metadata.getReceivedColumnName(), "a");
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Cannot position a column after hidden column '\\$path'")
    public void testMoveColumnAfterHiddenColumn()
    {
        // A hidden column is not part of the column order, so nothing can be moved after it, exactly as for
        // ADD COLUMN; both statements share the check
        execute(setColumnPosition("a", new After(new Identifier(HIDDEN_COLUMN_NAME, true))));
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Column 'missing' does not exist")
    public void testMoveMissingColumn()
    {
        execute(setColumnPosition("missing", new First()));
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Column 'missing' does not exist")
    public void testMoveColumnAfterMissingColumn()
    {
        execute(setColumnPosition("a", new After(identifier("missing"))));
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Column 'a' cannot be moved after itself")
    public void testMoveColumnAfterItself()
    {
        execute(setColumnPosition("a", new After(identifier("a"))));
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Column 'a' cannot be moved after itself")
    public void testMoveColumnAfterItselfDifferingByCase()
    {
        // Both names are normalized, so a target that differs from the column only by case is the column itself
        execute(setColumnPosition("a", new After(identifier("A"))));
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Cannot move hidden column")
    public void testMoveHiddenColumn()
    {
        execute(setColumnPosition(new Identifier(HIDDEN_COLUMN_NAME, true), new First()));
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*Table 'catalog.schema.test_table' does not exist")
    public void testMissingTable()
    {
        metadata = new MockMetadata(metadata.getCatalogHandle(), false);
        execute(setColumnPosition("a", new First()));
    }

    @Test
    public void testMissingTableWithIfExists()
    {
        metadata = new MockMetadata(metadata.getCatalogHandle(), false);
        execute(new SetColumnPosition(QualifiedName.of(TABLE_NAME), identifier("a"), new First(), true));
        assertNull(metadata.getReceivedPosition());
    }

    private static SetColumnPosition setColumnPosition(String column, com.facebook.presto.sql.tree.ColumnPosition position)
    {
        return setColumnPosition(identifier(column), position);
    }

    private static SetColumnPosition setColumnPosition(Identifier column, com.facebook.presto.sql.tree.ColumnPosition position)
    {
        return new SetColumnPosition(QualifiedName.of(TABLE_NAME), column, position, false);
    }

    private void execute(SetColumnPosition statement)
    {
        getFutureValue(new SetColumnPositionTask().execute(statement, transactionManager, metadata, new AllowAllAccessControl(), testSession, emptyList(), warningCollector, ""));
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final ConnectorId catalogHandle;
        private final Optional<TableHandle> tableHandle;
        private ColumnPosition receivedPosition;
        private String receivedColumnName;

        public MockMetadata(ConnectorId catalogHandle, boolean tableExists)
        {
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.tableHandle = tableExists ?
                    Optional.of(new TableHandle(catalogHandle, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty())) :
                    Optional.empty();
        }

        @Override
        public void setColumnPosition(Session session, TableHandle tableHandle, ColumnHandle column, ColumnPosition position)
        {
            this.receivedPosition = position;
            this.receivedColumnName = ((TestingColumnHandle) column).getName();
        }

        public ConnectorId getCatalogHandle()
        {
            return catalogHandle;
        }

        public ColumnPosition getReceivedPosition()
        {
            return receivedPosition;
        }

        public String getReceivedColumnName()
        {
            return receivedColumnName;
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
                    return tableHandle;
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
            // As in a real connector, the hidden column is part of the map, so the task has to filter it out itself
            return ImmutableMap.of(
                    "a", new TestingColumnHandle("a"),
                    "b", new TestingColumnHandle("b"),
                    HIDDEN_COLUMN_NAME, new TestingColumnHandle(HIDDEN_COLUMN_NAME));
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            String name = ((TestingColumnHandle) columnHandle).getName();
            return ColumnMetadata.builder()
                    .setName(name)
                    .setType(BIGINT)
                    .setHidden(name.startsWith("$"))
                    .build();
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
