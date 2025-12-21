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
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MaterializedViewPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.procedure.ProcedureRegistry;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.testing.TestProcedureRegistry;
import com.facebook.presto.tracing.TracingConfig;
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
import static com.facebook.presto.spi.session.PropertyMetadata.durationProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.base.Throwables.getRootCause;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
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
    private SessionPropertyManager sessionPropertyManager;

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

        MaterializedViewPropertyManager materializedViewPropertyManager = new MaterializedViewPropertyManager();
        materializedViewPropertyManager.addProperties(testCatalog.getConnectorId(), ImmutableList.of(
                stringProperty("storage_schema", "Schema for the materialized view storage table", null, false),
                stringProperty("storage_table", "Custom name for the materialized view storage table", null, false),
                stringProperty("stale_read_behavior", "Behavior when reading from a stale materialized view", null, false),
                durationProperty("staleness_window", "Staleness window for materialized view", null, false),
                stringProperty("refresh_type", "Refresh type for materialized view", null, false)));

        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();

        sessionPropertyManager = createSessionPropertyManager();

        transactionManager = createTestTransactionManager(catalogManager);
        testSession = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();

        accessControl = new AllowAllAccessControl();

        executorService = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        metadata = new MockMetadata(
                functionAndTypeManager,
                new TestProcedureRegistry(),
                tablePropertyManager,
                columnPropertyManager,
                materializedViewPropertyManager,
                testCatalog.getConnectorId());
    }

    @Test
    public void testCreateMaterializedViewNotExistsTrue()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        WarningCollector warningCollector = createWarningCollector(sql, testSession);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);
        getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, testSession, emptyList(), warningCollector, sql));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 1);
    }

    @Test
    public void testCreateMaterializedViewExistsFalse()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW %s AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_B, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        WarningCollector warningCollector = createWarningCollector(sql, testSession);
        try {
            getFutureValue(new CreateMaterializedViewTask(parser).execute(statement, transactionManager, metadata, accessControl, testSession, emptyList(), warningCollector, sql));
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

    @Test
    public void testCreateMaterializedViewWithDefinerSecurity()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW %s SECURITY DEFINER AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        Session sessionWithNonLegacyMV = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", "false")
                .build();

        WarningCollector warningCollector = createWarningCollector(sql, sessionWithNonLegacyMV);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);
        getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, sessionWithNonLegacyMV, emptyList(), warningCollector, sql));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 1);
        MaterializedViewDefinition createdView = metadata.getLastCreatedMaterializedViewDefinition();
        assertTrue(createdView.getOwner().isPresent(), "DEFINER security should have owner set");
        assertEquals(createdView.getOwner().get(), sessionWithNonLegacyMV.getUser());
    }

    @Test
    public void testCreateMaterializedViewWithInvokerSecurity()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW %s SECURITY INVOKER AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        Session sessionWithNonLegacyMV = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", "false")
                .build();

        WarningCollector warningCollector = createWarningCollector(sql, sessionWithNonLegacyMV);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);
        getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, sessionWithNonLegacyMV, emptyList(), warningCollector, sql));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 1);
        MaterializedViewDefinition createdView = metadata.getLastCreatedMaterializedViewDefinition();
        assertTrue(createdView.getOwner().isPresent(), "INVOKER security should have owner set");
    }

    @Test
    public void testCreateMaterializedViewWithDefaultDefinerSecurity()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW %s AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        Session sessionWithNonLegacyMV = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", "false")
                .setSystemProperty("default_view_security_mode", "DEFINER")
                .build();

        WarningCollector warningCollector = createWarningCollector(sql, sessionWithNonLegacyMV);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);
        getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, sessionWithNonLegacyMV, emptyList(), warningCollector, sql));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 1);
        MaterializedViewDefinition createdView = metadata.getLastCreatedMaterializedViewDefinition();
        assertTrue(createdView.getOwner().isPresent(), "Default DEFINER security should have owner set");
        assertEquals(createdView.getOwner().get(), sessionWithNonLegacyMV.getUser());
    }

    @Test
    public void testCreateMaterializedViewWithDefaultInvokerSecurity()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW %s AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        Session sessionWithNonLegacyMV = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", "false")
                .setSystemProperty("default_view_security_mode", "INVOKER")
                .build();

        WarningCollector warningCollector = createWarningCollector(sql, sessionWithNonLegacyMV);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);
        getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, sessionWithNonLegacyMV, emptyList(), warningCollector, sql));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 1);
        MaterializedViewDefinition createdView = metadata.getLastCreatedMaterializedViewDefinition();
        assertTrue(createdView.getOwner().isPresent(), "Default INVOKER security should have owner set");
    }

    @Test
    public void testCreateMaterializedViewWithSecurityInLegacyMode()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW %s SECURITY INVOKER AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        Session sessionWithLegacyMV = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", "true")
                .build();

        WarningCollector warningCollector = createWarningCollector(sql, sessionWithLegacyMV);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);

        SemanticException exception = expectThrows(SemanticException.class, () ->
                getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, sessionWithLegacyMV, emptyList(), warningCollector, sql)));
        assertTrue(exception.getMessage().contains("SECURITY clause is not supported when legacy_materialized_views is enabled"));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 0);
    }

    @Test
    public void testCreateMaterializedViewInLegacyModeAlwaysHasOwner()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW %s AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        Session sessionWithLegacyMV = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", "true")
                .build();

        WarningCollector warningCollector = createWarningCollector(sql, sessionWithLegacyMV);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);
        getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, sessionWithLegacyMV, emptyList(), warningCollector, sql));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 1);
        MaterializedViewDefinition createdView = metadata.getLastCreatedMaterializedViewDefinition();
        assertTrue(createdView.getOwner().isPresent(), "Legacy mode should always have owner set");
        assertEquals(createdView.getOwner().get(), sessionWithLegacyMV.getUser());
    }

    @Test
    public void testCreateMaterializedViewWithMaterializedViewProperties()
    {
        SqlParser parser = new SqlParser();
        String sql = format(
                "CREATE MATERIALIZED VIEW %s " +
                        "WITH (stale_read_behavior = 'FAIL', " +
                        "staleness_window = '1h', " +
                        "refresh_type = 'FULL') " +
                        "AS SELECT 2021 AS col_0 FROM %s",
                MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        Session sessionWithNonLegacyMV = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", "false")
                .build();

        WarningCollector warningCollector = createWarningCollector(sql, sessionWithNonLegacyMV);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);
        getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, sessionWithNonLegacyMV, emptyList(), warningCollector, sql));

        assertEquals(metadata.getCreateMaterializedViewCallCount(), 1);

        ConnectorTableMetadata viewMetadata = metadata.getLastCreatedViewMetadata();
        Map<String, Object> properties = viewMetadata.getProperties();

        assertEquals(properties.get("stale_read_behavior"), "FAIL");
        assertEquals(properties.get("staleness_window").toString(), "1.00h");
        assertEquals(properties.get("refresh_type"), "FULL");
    }

    @Test
    public void testCreateMaterializedViewWithInvalidDefaultViewSecurityMode()
    {
        SqlParser parser = new SqlParser();
        String sql = format("CREATE MATERIALIZED VIEW %s AS SELECT 2021 AS col_0 FROM %s", MATERIALIZED_VIEW_A, TABLE_A);
        CreateMaterializedView statement = (CreateMaterializedView) parser.createStatement(sql, ParsingOptions.builder().build());

        Session sessionWithInvalidSecurityMode = testSessionBuilder(sessionPropertyManager)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", "false")
                .setSystemProperty("default_view_security_mode", "INVALID")
                .build();

        WarningCollector warningCollector = createWarningCollector(sql, sessionWithInvalidSecurityMode);
        CreateMaterializedViewTask createMaterializedViewTask = new CreateMaterializedViewTask(parser);

        Exception exception = expectThrows(Exception.class, () ->
                getFutureValue(createMaterializedViewTask.execute(statement, transactionManager, metadata, accessControl, sessionWithInvalidSecurityMode, emptyList(), warningCollector, sql)));

        Throwable rootCause = getRootCause(exception);
        assertTrue(rootCause instanceof IllegalArgumentException,
                "Expected IllegalArgumentException but got: " + rootCause.getClass().getName());
        assertTrue(rootCause.getMessage().contains("INVALID") || rootCause.getMessage().contains("ViewSecurity"),
                "Exception message should mention INVALID or ViewSecurity but was: " + rootCause.getMessage());
        assertEquals(metadata.getCreateMaterializedViewCallCount(), 0);
    }

    private static SessionPropertyManager createSessionPropertyManager()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setAllowLegacyMaterializedViewsToggle(true);

        return SessionPropertyManager.createTestingSessionPropertyManager(
                new com.facebook.presto.SystemSessionProperties(
                        new QueryManagerConfig(),
                        new TaskManagerConfig(),
                        new MemoryManagerConfig(),
                        featuresConfig,
                        new FunctionsConfig(),
                        new NodeMemoryConfig(),
                        new WarningCollectorConfig(),
                        new NodeSchedulerConfig(),
                        new NodeSpillConfig(),
                        new TracingConfig(),
                        new CompilerConfig(),
                        new HistoryBasedOptimizationConfig()).getSessionProperties(),
                featuresConfig,
                new JavaFeaturesConfig(),
                new NodeSpillConfig());
    }

    private WarningCollector createWarningCollector(String sql, Session session)
    {
        QueryStateMachine stateMachine = QueryStateMachine.begin(
                sql,
                Optional.empty(),
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                Optional.empty(),
                false,
                transactionManager,
                accessControl,
                executorService,
                metadata,
                WarningCollector.NOOP);
        return stateMachine.getWarningCollector();
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final ProcedureRegistry procedureRegistry;
        private final TablePropertyManager tablePropertyManager;
        private final ColumnPropertyManager columnPropertyManager;
        private final MaterializedViewPropertyManager materializedViewPropertyManager;
        private final ConnectorId catalogHandle;

        private final List<ConnectorTableMetadata> materializedViews = new CopyOnWriteArrayList<>();
        private MaterializedViewDefinition lastCreatedMaterializedViewDefinition;

        public MockMetadata(
                FunctionAndTypeManager functionAndTypeManager,
                ProcedureRegistry procedureRegistry,
                TablePropertyManager tablePropertyManager,
                ColumnPropertyManager columnPropertyManager,
                MaterializedViewPropertyManager materializedViewPropertyManager,
                ConnectorId catalogHandle)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.procedureRegistry = requireNonNull(procedureRegistry, "procedureRegistry is null");
            this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
            this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
            this.materializedViewPropertyManager = requireNonNull(materializedViewPropertyManager, "materializedViewPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        }

        @Override
        public void createMaterializedView(Session session, String catalogName, ConnectorTableMetadata viewMetadata, MaterializedViewDefinition viewDefinition, boolean ignoreExisting)
        {
            // Check if materialized view already exists (MATERIALIZED_VIEW_B always exists for testing)
            if (viewMetadata.getTable().getTableName().equals(MATERIALIZED_VIEW_B) && !ignoreExisting) {
                throw new PrestoException(ALREADY_EXISTS, "Materialized view already exists");
            }
            this.materializedViews.add(viewMetadata);
            this.lastCreatedMaterializedViewDefinition = viewDefinition;
        }

        public int getCreateMaterializedViewCallCount()
        {
            return materializedViews.size();
        }

        public MaterializedViewDefinition getLastCreatedMaterializedViewDefinition()
        {
            return lastCreatedMaterializedViewDefinition;
        }

        public ConnectorTableMetadata getLastCreatedViewMetadata()
        {
            return materializedViews.isEmpty() ? null : materializedViews.get(materializedViews.size() - 1);
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
        public MaterializedViewPropertyManager getMaterializedViewPropertyManager()
        {
            return materializedViewPropertyManager;
        }

        @Override
        public FunctionAndTypeManager getFunctionAndTypeManager()
        {
            return functionAndTypeManager;
        }

        @Override
        public ProcedureRegistry getProcedureRegistry()
        {
            return procedureRegistry;
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return functionAndTypeManager.getType(signature);
        }

        @Override
        public MetadataResolver getMetadataResolver(Session session)
        {
            return new MetadataResolver()
            {
                @Override
                public boolean catalogExists(String catalogName)
                {
                    return false;
                }

                @Override
                public boolean schemaExists(CatalogSchemaName schemaName)
                {
                    return false;
                }

                @Override
                public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
                {
                    return getOptionalTableHandle(tableName);
                }

                @Override
                public List<ColumnMetadata> getColumns(TableHandle tableHandle)
                {
                    return emptyList();
                }

                @Override
                public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
                {
                    return emptyMap();
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

        private Optional<TableHandle> getOptionalTableHandle(QualifiedObjectName tableName)
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
