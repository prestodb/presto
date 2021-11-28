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
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCreateMaterializedViewTask
{
    private static final String CATALOG_NAME = "catalog";
    private static final String SCHEMA_NAME = "schema";
    private static final String MATERIALIZED_VIEW_A = "materialized_view_a";
    private static final String MATERIALIZED_VIEW_B = "materialized_view_b";
    private static final String TABLE_A = "table_a";

    private TransactionManager transactionManager;
    private Session testSession;

    private AccessControl accessControl;

    private ExecutorService executorService;

    private MockMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        Catalog testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);

        TablePropertyManager tablePropertyManager = new TablePropertyManager();
        tablePropertyManager.addProperties(testCatalog.getConnectorId(),
                ImmutableList.of(stringProperty("baz", "test property", null, false)));

        ColumnPropertyManager columnPropertyManager = new ColumnPropertyManager();
        columnPropertyManager.addProperties(testCatalog.getConnectorId(), ImmutableList.of());

        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();

        transactionManager = createTestTransactionManager(catalogManager);
        testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();

        accessControl = new AllowAllAccessControl();

        executorService = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        metadata = new MockMetadata(
                functionAndTypeManager,
                tablePropertyManager,
                columnPropertyManager,
                testCatalog.getConnectorId());
    }

    @Test
    public void testCreateMaterializedViewNotExistsTrue()
    {
        SqlParser parser = new SqlParser();
        String sql = String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                sql,
                testSession,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                Optional.empty(),
                false,
                transactionManager,
                accessControl,
                executorService,
                metadata,
                WarningCollector.NOOP);

        getFutureValue(new CreateMaterializedViewTask(parser).execute(statement, transactionManager, metadata, accessControl, stateMachine, emptyList()));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 1);
    }

    @Test
    public void testCreateMaterializedViewExistsFalse()
    {
        SqlParser parser = new SqlParser();
        String sql = String.format("CREATE MATERIALIZED VIEW %s AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_B, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                sql,
                testSession,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                Optional.empty(),
                false,
                transactionManager,
                accessControl,
                executorService,
                metadata,
                WarningCollector.NOOP);

        try {
            getFutureValue(new CreateMaterializedViewTask(parser).execute(statement, transactionManager, metadata, accessControl, stateMachine, emptyList()));
            fail("expected exception");
        }
        catch (RuntimeException e) {
            // Expected
            assertTrue(e instanceof PrestoException);
            PrestoException prestoException = (PrestoException) e;
            assertEquals(prestoException.getErrorCode(), ALREADY_EXISTS.toErrorCode());
        }

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 0);
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final TablePropertyManager tablePropertyManager;
        private final ColumnPropertyManager columnPropertyManager;
        private final ConnectorId catalogHandle;

        private final List<ConnectorTableMetadata> materializedViews = new CopyOnWriteArrayList<>();

        public MockMetadata(
                FunctionAndTypeManager functionAndTypeManager,
                TablePropertyManager tablePropertyManager,
                ColumnPropertyManager columnPropertyManager,
                ConnectorId catalogHandle)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
            this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        }

        @Override
        public void createMaterializedView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, ConnectorMaterializedViewDefinition viewDefinition, boolean ignoreExisting)
        {
            if (!ignoreExisting) {
                throw new PrestoException(ALREADY_EXISTS, "Materialized view already exists");
            }
            this.materializedViews.add(viewMetadata);
        }

        public int getCreateMaterializedViewCallCount()
        {
            return materializedViews.size();
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
        public FunctionAndTypeManager getFunctionAndTypeManager()
        {
            return functionAndTypeManager;
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return functionAndTypeManager.getType(signature);
        }

        @Override
        public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
        {
            if (tableName.getObjectName().equals(MATERIALIZED_VIEW_A)) {
                return Optional.empty();
            }
            if (tableName.getObjectName().equals(TABLE_A)) {
                return Optional.of(new TableHandle(
                        catalogHandle,
                        new ConnectorTableHandle() {},
                        new ConnectorTransactionHandle() {},
                        Optional.empty()));
            }
            return Optional.empty();
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            return emptyMap();
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            return new TableMetadata(
                    catalogHandle,
                    new ConnectorTableMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_A), emptyList()));
        }

        @Override
        public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
        {
            return Optional.empty();
        }

        @Override
        public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
        {
            return Optional.empty();
        }

        @Override
        public BlockEncodingSerde getBlockEncodingSerde()
        {
            return new TestingBlockEncodingSerde();
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
