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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MaterializedViewPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.tree.SetProperties;
import com.facebook.presto.tracing.TracingConfig;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_LEGACY_MATERIALIZED_VIEW_SET_PROPERTIES;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

@Test(singleThreaded = true)
public class TestSetPropertiesTask
{
    private static final String CATALOG_NAME = "catalog";
    private static final String SCHEMA_NAME = "schema";
    private static final String MATERIALIZED_VIEW_NAME = "mv";
    private static final String SQL = "ALTER MATERIALIZED VIEW mv SET PROPERTIES (foo = 'bar')";

    private TransactionManager transactionManager;
    private SessionPropertyManager sessionPropertyManager;
    private FunctionAndTypeManager functionAndTypeManager;
    private MaterializedViewPropertyManager materializedViewPropertyManager;
    private AccessControl accessControl;
    private ConnectorId connectorId;

    @BeforeMethod
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        Catalog testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        connectorId = testCatalog.getConnectorId();

        materializedViewPropertyManager = new MaterializedViewPropertyManager();
        materializedViewPropertyManager.addProperties(connectorId, ImmutableList.of(
                stringProperty("foo", "test property", null, false)));

        functionAndTypeManager = createTestFunctionAndTypeManager();
        sessionPropertyManager = createSessionPropertyManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();
    }

    @Test
    public void testAllowedInLegacyModeWhenCapabilityAdvertised()
    {
        MockMetadata metadata = new MockMetadata(ImmutableSet.of(SUPPORTS_LEGACY_MATERIALIZED_VIEW_SET_PROPERTIES));
        getFutureValue(new SetPropertiesTask().execute(parse(SQL), transactionManager, metadata, accessControl, session(true), emptyList(), WarningCollector.NOOP, SQL));
        assertEquals(metadata.getSetMaterializedViewPropertiesCallCount(), 1);
    }

    @Test
    public void testRejectedInLegacyModeWhenCapabilityAbsent()
    {
        MockMetadata metadata = new MockMetadata(ImmutableSet.of());
        PrestoException e = expectThrows(PrestoException.class, () ->
                new SetPropertiesTask().execute(parse(SQL), transactionManager, metadata, accessControl, session(true), emptyList(), WarningCollector.NOOP, SQL));
        assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        assertEquals(metadata.getSetMaterializedViewPropertiesCallCount(), 0);
    }

    @Test
    public void testAllowedInNonLegacyModeWithoutCapability()
    {
        MockMetadata metadata = new MockMetadata(ImmutableSet.of());
        getFutureValue(new SetPropertiesTask().execute(parse(SQL), transactionManager, metadata, accessControl, session(false), emptyList(), WarningCollector.NOOP, SQL));
        assertEquals(metadata.getSetMaterializedViewPropertiesCallCount(), 1);
    }

    private Session session(boolean legacyMaterializedViews)
    {
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA_NAME)
                .setTransactionId(transactionManager.beginTransaction(false))
                .setSystemProperty("legacy_materialized_views", Boolean.toString(legacyMaterializedViews))
                .build();
    }

    private static SetProperties parse(String sql)
    {
        return (SetProperties) new SqlParser().createStatement(sql, ParsingOptions.builder().build());
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

    private class MockMetadata
            extends AbstractMockMetadata
    {
        private final Set<ConnectorCapabilities> capabilities;
        private int setMaterializedViewPropertiesCallCount;

        MockMetadata(Set<ConnectorCapabilities> capabilities)
        {
            this.capabilities = requireNonNull(capabilities, "capabilities is null");
        }

        int getSetMaterializedViewPropertiesCallCount()
        {
            return setMaterializedViewPropertiesCallCount;
        }

        @Override
        public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
        {
            return CATALOG_NAME.equals(catalogName) ? Optional.of(connectorId) : Optional.empty();
        }

        @Override
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId catalogName)
        {
            return capabilities;
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
        public Type getType(TypeSignature signature)
        {
            return functionAndTypeManager.getType(signature);
        }

        @Override
        public BlockEncodingSerde getBlockEncodingSerde()
        {
            return new TestingBlockEncodingSerde();
        }

        @Override
        public void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Object> properties)
        {
            setMaterializedViewPropertiesCallCount++;
        }

        @Override
        public MetadataResolver getMetadataResolver(Session session)
        {
            return new MetadataResolver()
            {
                @Override
                public boolean catalogExists(String catalogName)
                {
                    return CATALOG_NAME.equals(catalogName);
                }

                @Override
                public boolean schemaExists(CatalogSchemaName schemaName)
                {
                    return CATALOG_NAME.equals(schemaName.getCatalogName()) && SCHEMA_NAME.equals(schemaName.getSchemaName());
                }

                @Override
                public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
                {
                    return Optional.empty();
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
                    if (!MATERIALIZED_VIEW_NAME.equals(viewName.getObjectName())) {
                        return Optional.empty();
                    }
                    return Optional.of(new MaterializedViewDefinition(
                            "SELECT 1",
                            SCHEMA_NAME,
                            MATERIALIZED_VIEW_NAME,
                            emptyList(),
                            Optional.empty(),
                            Optional.empty(),
                            emptyList(),
                            emptyList(),
                            Optional.empty()));
                }
            };
        }
    }
}
