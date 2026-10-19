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
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.testing.TestingWarningCollector;
import com.facebook.presto.testing.TestingWarningCollectorConfig;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.ALTER_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDropColumnTask
{
    private static final String CATALOG_NAME = "catalog";
    private static final String TABLE_NAME = "test_table";

    private TransactionManager transactionManager;
    private Session testSession;
    private MockMetadata metadata;
    private TestingAccessControlManager accessControl;
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
        metadata = new MockMetadata(testCatalog.getConnectorId());
        accessControl = new TestingAccessControlManager(transactionManager);
    }

    @Test
    public void testTopLevelDropColumnCallsDropColumn()
    {
        execute(dropColumn(QualifiedName.of("x"), false));

        assertEquals(metadata.droppedColumn, new TestingColumnHandle("x"));
        assertEquals(metadata.dropColumnCalls, 1);
        assertEquals(metadata.dropFieldCalls, 0);
    }

    @Test
    public void testTopLevelDropColumnDeniedWhenDropColumnNotAllowed()
    {
        accessControl.deny(privilege(TABLE_NAME, DROP_COLUMN));
        assertThrows(AccessDeniedException.class, () -> execute(dropColumn(QualifiedName.of("x"), false)));
        // A denied statement must not reach the metadata layer
        assertEquals(metadata.dropColumnCalls, 0);
        assertEquals(metadata.dropFieldCalls, 0);
    }

    @Test
    public void testTopLevelDropColumnAllowedWhenOnlyAlterColumnDenied()
    {
        // Top-level DROP COLUMN requires DROP_COLUMN, not ALTER_COLUMN
        accessControl.deny(privilege(TABLE_NAME, ALTER_COLUMN));
        execute(dropColumn(QualifiedName.of("x"), false));

        assertEquals(metadata.droppedColumn, new TestingColumnHandle("x"));
        assertEquals(metadata.dropColumnCalls, 1);
        assertEquals(metadata.dropFieldCalls, 0);
    }

    @Test
    public void testNestedDropColumnDeniedWhenAlterColumnNotAllowed()
    {
        accessControl.deny(privilege(TABLE_NAME, ALTER_COLUMN));
        assertThrows(AccessDeniedException.class, () -> execute(dropColumn(QualifiedName.of("info", "age"), false)));
        // A denied statement must not reach the metadata layer
        assertEquals(metadata.dropColumnCalls, 0);
        assertEquals(metadata.dropFieldCalls, 0);
    }

    @Test
    public void testNestedDropColumnAllowedWhenOnlyDropColumnDenied()
    {
        // Nested DROP COLUMN requires ALTER_COLUMN, not DROP_COLUMN
        accessControl.deny(privilege(TABLE_NAME, DROP_COLUMN));
        execute(dropColumn(QualifiedName.of("info", "age"), false));

        assertEquals(metadata.receivedFieldPath, ImmutableList.of("info", "age"));
        assertEquals(metadata.dropFieldCalls, 1);
        assertEquals(metadata.dropColumnCalls, 0);
    }

    @Test
    public void testNestedDropColumnPassesCorrectArgumentsToDropField()
    {
        // Original casing in the statement must be normalized before it reaches the connector
        QualifiedName nestedName = QualifiedName.of(ImmutableList.of(
                new Identifier("Outer", true),
                new Identifier("Inner", true),
                new Identifier("Age", true)));

        // isColumnExists=true so ignoreNonExistent must be forwarded as true
        execute(dropColumn(nestedName, true));
        assertEquals(metadata.receivedFieldPath, ImmutableList.of("outer", "inner", "age"));
        assertTrue(metadata.receivedFieldIgnoreNonExistent);

        // isColumnExists=false so ignoreNonExistent must be forwarded as false
        execute(dropColumn(nestedName, false));
        assertEquals(metadata.receivedFieldPath, ImmutableList.of("outer", "inner", "age"));
        assertFalse(metadata.receivedFieldIgnoreNonExistent);

        assertEquals(metadata.dropFieldCalls, 2);
        assertEquals(metadata.dropColumnCalls, 0);
        assertNull(metadata.droppedColumn);
    }

    private static DropColumn dropColumn(QualifiedName column, boolean columnExists)
    {
        return new DropColumn(QualifiedName.of(TABLE_NAME), column, false, columnExists);
    }

    private void execute(DropColumn statement)
    {
        getFutureValue(new DropColumnTask().execute(statement, transactionManager, metadata, accessControl, testSession, emptyList(), warningCollector, ""));
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final ConnectorId catalogHandle;
        private final TableHandle tableHandle;
        private ColumnHandle droppedColumn;
        private int dropColumnCalls;
        private List<String> receivedFieldPath;
        private boolean receivedFieldIgnoreNonExistent;
        private int dropFieldCalls;

        public MockMetadata(ConnectorId catalogHandle)
        {
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.tableHandle = new TableHandle(catalogHandle, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty());
        }

        @Override
        public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
        {
            this.droppedColumn = column;
            this.dropColumnCalls++;
        }

        @Override
        public void dropField(Session session, TableHandle tableHandle, List<String> fieldPath, boolean ignoreNonExistent)
        {
            this.receivedFieldPath = fieldPath;
            this.receivedFieldIgnoreNonExistent = ignoreNonExistent;
            this.dropFieldCalls++;
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
            // Two visible columns, so the "Cannot drop the only column in a table" check does not apply to "x"
            return ImmutableMap.of(
                    "x", new TestingColumnHandle("x"),
                    "y", new TestingColumnHandle("y"));
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return column(((TestingColumnHandle) columnHandle).getName());
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            return new TableMetadata(
                    catalogHandle,
                    new ConnectorTableMetadata(new SchemaTableName("schema", TABLE_NAME), ImmutableList.of(column("x"), column("y"))));
        }

        private static ColumnMetadata column(String name)
        {
            return ColumnMetadata.builder()
                    .setName(name)
                    .setType(BIGINT)
                    .build();
        }
    }
}
