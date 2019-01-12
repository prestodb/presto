
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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.connector.ConnectorId;
import io.prestosql.metadata.AbstractMockMetadata;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.tree.ColumnDefinition;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.testing.TestingSession.createBogusTestingCatalog;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
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
        metadata = new MockMetadata(typeManager,
                tablePropertyManager,
                columnPropertyManager,
                testCatalog.getConnectorId());
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

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final TypeManager typeManager;
        private final TablePropertyManager tablePropertyManager;
        private final ColumnPropertyManager columnPropertyManager;
        private final ConnectorId catalogHandle;
        private AtomicInteger createTableCallCount = new AtomicInteger();

        public MockMetadata(
                TypeManager typeManager,
                TablePropertyManager tablePropertyManager,
                ColumnPropertyManager columnPropertyManager,
                ConnectorId catalogHandle)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
            this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        }

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
        {
            createTableCallCount.incrementAndGet();
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
            return createTableCallCount.get();
        }

        @Override
        public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
        {
            throw new UnsupportedOperationException();
        }
    }
}
