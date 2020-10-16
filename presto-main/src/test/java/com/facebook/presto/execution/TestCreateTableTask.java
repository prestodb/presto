
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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.collect.Sets.immutableEnumSet;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCreateTableTask
{
    private static final String CATALOG_NAME = "catalog";
    private CatalogManager catalogManager;
    private FunctionAndTypeManager functionAndTypeManager;
    private TransactionManager transactionManager;
    private TablePropertyManager tablePropertyManager;
    private ColumnPropertyManager columnPropertyManager;
    private Catalog testCatalog;
    private Session testSession;
    private MockMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        catalogManager = new CatalogManager();
        functionAndTypeManager = createTestFunctionAndTypeManager();
        transactionManager = createTestTransactionManager(catalogManager);
        tablePropertyManager = new TablePropertyManager();
        columnPropertyManager = new ColumnPropertyManager();
        testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        tablePropertyManager.addProperties(testCatalog.getConnectorId(),
                ImmutableList.of(stringProperty("baz", "test property", null, false)));
        columnPropertyManager.addProperties(testCatalog.getConnectorId(), ImmutableList.of());
        testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        metadata = new MockMetadata(
                functionAndTypeManager,
                tablePropertyManager,
                columnPropertyManager,
                testCatalog.getConnectorId(),
                emptySet());
    }

    @Test
    public void testCreateTableNotExistsTrue()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition(identifier("a"), "BIGINT", true, emptyList(), Optional.empty())),
                true,
                ImmutableList.of(),
                Optional.empty());

        getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
        assertEquals(metadata.getCreateTableCallCount(), 1);
    }

    @Test
    public void testCreateTableNotExistsFalse()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition(identifier("a"), "BIGINT", true, emptyList(), Optional.empty())),
                false,
                ImmutableList.of(),
                Optional.empty());

        try {
            getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
            fail("expected exception");
        }
        catch (RuntimeException e) {
            // Expected
            assertTrue(e instanceof PrestoException);
            PrestoException prestoException = (PrestoException) e;
            assertEquals(prestoException.getErrorCode(), ALREADY_EXISTS.toErrorCode());
        }
        assertEquals(metadata.getCreateTableCallCount(), 1);
    }

    @Test
    public void testCreateWithNotNullColumns()
    {
        metadata.setConnectorCapabilities(NOT_NULL_COLUMN_CONSTRAINT);
        List<TableElement> inputColumns = ImmutableList.of(
                new ColumnDefinition(identifier("a"), "DATE", true, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("b"), "VARCHAR", false, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("c"), "VARBINARY", false, emptyList(), Optional.empty()));
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"), inputColumns, true, ImmutableList.of(), Optional.empty());

        getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
        assertEquals(metadata.getCreateTableCallCount(), 1);
        List<ColumnMetadata> columns = metadata.getReceivedTableMetadata().get(0).getColumns();
        assertEquals(columns.size(), 3);

        assertEquals(columns.get(0).getName(), "a");
        assertEquals(columns.get(0).getType().getDisplayName().toUpperCase(ENGLISH), "DATE");
        assertTrue(columns.get(0).isNullable());

        assertEquals(columns.get(1).getName(), "b");
        assertEquals(columns.get(1).getType().getDisplayName().toUpperCase(ENGLISH), "VARCHAR");
        assertFalse(columns.get(1).isNullable());

        assertEquals(columns.get(2).getName(), "c");
        assertEquals(columns.get(2).getType().getDisplayName().toUpperCase(ENGLISH), "VARBINARY");
        assertFalse(columns.get(2).isNullable());
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = ".*does not support non-null column for column name 'b'")
    public void testCreateWithUnsupportedConnectorThrowsWhenNotNull()
    {
        List<TableElement> inputColumns = ImmutableList.of(
                new ColumnDefinition(identifier("a"), "DATE", true, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("b"), "VARCHAR", false, emptyList(), Optional.empty()),
                new ColumnDefinition(identifier("c"), "VARBINARY", false, emptyList(), Optional.empty()));
        CreateTable statement = new CreateTable(
                QualifiedName.of("test_table"),
                inputColumns,
                true,
                ImmutableList.of(),
                Optional.empty());

        getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final TablePropertyManager tablePropertyManager;
        private final ColumnPropertyManager columnPropertyManager;
        private final ConnectorId catalogHandle;
        private final List<ConnectorTableMetadata> tables = new CopyOnWriteArrayList<>();
        private Set<ConnectorCapabilities> connectorCapabilities;

        public MockMetadata(
                FunctionAndTypeManager functionAndTypeManager,
                TablePropertyManager tablePropertyManager,
                ColumnPropertyManager columnPropertyManager,
                ConnectorId catalogHandle,
                Set<ConnectorCapabilities> connectorCapabilities)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
            this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.connectorCapabilities = requireNonNull(immutableEnumSet(connectorCapabilities), "connectorCapabilities is null");
        }

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
        {
            tables.add(tableMetadata);
            if (!ignoreExisting) {
                throw new PrestoException(ALREADY_EXISTS, "Table already exists");
            }
        }

        @Override
        public TablePropertyManager getTablePropertyManager()
        {
            return tablePropertyManager;
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

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
        {
            return Optional.empty();
        }

        public int getCreateTableCallCount()
        {
            return tables.size();
        }

        public List<ConnectorTableMetadata> getReceivedTableMetadata()
        {
            return tables;
        }

        @Override
        public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId catalogName)
        {
            return connectorCapabilities;
        }

        public void setConnectorCapabilities(ConnectorCapabilities... connectorCapabilities)
        {
            this.connectorCapabilities = immutableEnumSet(ImmutableList.copyOf(connectorCapabilities));
        }
    }
}
