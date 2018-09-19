
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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCreateTableTask
{
    private static final String CATALOG_NAME = "catalog";
    private CatalogManager catalogManager;
    private TypeManager typeManager;
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
        typeManager = new TypeRegistry();
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
        FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        metadata = new MockMetadata(typeManager,
                tablePropertyManager,
                columnPropertyManager,
                testCatalog.getConnectorId(),
                functionRegistry);
    }

    @Test
    public void testCreateTableNotExistsTrue()
    {
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition(identifier("a"), "BIGINT", emptyList(), Optional.empty())),
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
                ImmutableList.of(new ColumnDefinition(identifier("a"), "BIGINT", emptyList(), Optional.empty())),
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
    public void testStringColumnsWithDefaults()
    {
        ImmutableList<ColumnDefinition> inputColumns = ImmutableList.of(
                new ColumnDefinition(identifier("a"), "DATE", emptyList(), Optional.empty(), true, Optional.of(new StringLiteral("2011-01-1"))),
                new ColumnDefinition(identifier("b"), "VARCHAR", emptyList(), Optional.empty(), false, Optional.of(new GenericLiteral("DATE", "2011-01-1"))),
                new ColumnDefinition(identifier("c"), "VARBINARY", emptyList(), Optional.empty(), false, Optional.of(new GenericLiteral("VARBINARY", "2011-01-1"))));
        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                inputColumns,
                true,
                ImmutableList.of(),
                Optional.empty());

        getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), testSession, emptyList()));
        assertEquals(metadata.getCreateTableCallCount(), 1);
        List<ColumnMetadata> columns = metadata.getReceivedTableMetadata().get(0).getColumns();
        assertEquals(columns.size(), 3);

        assertEquals(columns.get(0).getName(), inputColumns.get(0).getName().getValue());
        assertEquals(columns.get(0).getType().getDisplayName().toUpperCase(ENGLISH), inputColumns.get(0).getType());
        assertEquals(columns.get(0).isNullable(), inputColumns.get(0).isNullable());
        assertEquals(columns.get(0).getDefaultValue(), 14975L);

        assertEquals(columns.get(1).getName(), inputColumns.get(1).getName().getValue());
        assertEquals(columns.get(1).getType().getDisplayName().toUpperCase(ENGLISH), inputColumns.get(1).getType());
        assertEquals(columns.get(1).isNullable(), inputColumns.get(1).isNullable());
        assertEquals(((Slice) columns.get(1).getDefaultValue()).toStringUtf8(), "2011-01-01");

        assertEquals(columns.get(2).getName(), inputColumns.get(2).getName().getValue());
        assertEquals(columns.get(2).getType().getDisplayName().toUpperCase(ENGLISH), inputColumns.get(2).getType());
        assertEquals(columns.get(2).isNullable(), inputColumns.get(2).isNullable());
        assertEquals(((Slice) columns.get(2).getDefaultValue()).toStringUtf8(), "2011-01-1");
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final TypeManager typeManager;
        private final TablePropertyManager tablePropertyManager;
        private final ColumnPropertyManager columnPropertyManager;
        private final ConnectorId catalogHandle;
        private final FunctionRegistry functionRegistry;
        private List<ConnectorTableMetadata> metadatas = new CopyOnWriteArrayList<>();

        public MockMetadata(
                TypeManager typeManager,
                TablePropertyManager tablePropertyManager,
                ColumnPropertyManager columnPropertyManager,
                ConnectorId catalogHandle,
                FunctionRegistry functionRegistry)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
            this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
        }

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
        {
            metadatas.add(tableMetadata);
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
            return typeManager.getType(signature);
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
            return metadatas.size();
        }

        public List<ConnectorTableMetadata> getReceivedTableMetadata()
        {
            return metadatas;
        }

        @Override
        public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FunctionRegistry getFunctionRegistry()
        {
            return functionRegistry;
        }
    }
}
