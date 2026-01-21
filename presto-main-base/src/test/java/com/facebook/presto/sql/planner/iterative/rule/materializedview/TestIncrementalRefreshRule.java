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
package com.facebook.presto.sql.planner.iterative.rule.materializedview;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.ColumnMapping;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.RefreshMaterializedViewNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.TestingConnectorTransactionHandle;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.FULLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.PARTIALLY_MATERIALIZED;
import static com.facebook.presto.spi.security.ViewSecurity.DEFINER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.slice.Slices.utf8Slice;

/**
 * Tests for {@link IncrementalRefreshRule}.
 *
 * <p>These tests verify the plan optimization for incremental materialized view refresh,
 * which transforms a full refresh plan into a delta-only refresh when possible.
 */
@Test(singleThreaded = true)
public class TestIncrementalRefreshRule
        extends BaseRuleTest
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("test_catalog");
    private static final SchemaTableName BASE_TABLE = new SchemaTableName("test_schema", "base_table");
    private static final SchemaTableName MV_STORAGE_TABLE = new SchemaTableName("test_schema", "mv_storage");
    private static final QualifiedObjectName MV_NAME = new QualifiedObjectName("test_catalog", "test_schema", "test_mv");

    @Override
    @BeforeClass
    public void setUp()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setAllowLegacyMaterializedViewsToggle(true)
                .setLegacyMaterializedViews(false);

        Session tempSession = testSessionBuilder().setCatalog("local").setSchema("tiny").build();
        LocalQueryRunner queryRunner = new LocalQueryRunner(tempSession, featuresConfig, new FunctionsConfig());

        Session session = testSessionBuilder(queryRunner.getMetadata().getSessionPropertyManager())
                .setCatalog("local")
                .setSchema("tiny")
                .build();
        tester = new RuleTester(ImmutableList.of(), session, queryRunner, new TpchConnectorFactory(1));
    }

    @Test
    public void testFallsBackToFullRefreshWhenFullyMaterialized()
    {
        // When MV is fully materialized, rule fires but returns source unchanged (full refresh)
        Metadata metadata = new TestingMetadataForIncrementalRefresh(
                tester().getMetadata(),
                createSimpleMvDefinition(),
                new MaterializedViewStatus(FULLY_MATERIALIZED));

        tester().assertThat(new IncrementalRefreshRule(metadata))
                .on(this::buildRefreshPlan)
                .matches(values("id", "ds"));
    }

    @Test
    public void testFallsBackToFullRefreshWhenNoPartitionConstraints()
    {
        // Partially materialized but no partition info available (empty map)
        // Rule fires but returns source unchanged (full refresh)
        Metadata metadata = new TestingMetadataForIncrementalRefresh(
                tester().getMetadata(),
                createSimpleMvDefinition(),
                new MaterializedViewStatus(PARTIALLY_MATERIALIZED, ImmutableMap.of(), Optional.empty()));

        tester().assertThat(new IncrementalRefreshRule(metadata))
                .on(this::buildRefreshPlan)
                .matches(values("id", "ds"));
    }

    @Test
    public void testFallsBackToFullRefreshWhenMvDefinitionNotFound()
    {
        // Metadata returns empty Optional for MV definition
        // Rule fires but returns source unchanged (full refresh)
        Metadata metadata = new TestingMetadataWithMissingMvDefinition(tester().getMetadata());

        tester().assertThat(new IncrementalRefreshRule(metadata))
                .on(this::buildRefreshPlan)
                .matches(values("id", "ds"));
    }

    @Test
    public void testFallsBackToFullRefreshWhenStaleColumnsHaveNoMapping()
    {
        // Stale partition has column 'unmapped_col' which doesn't exist in column mappings
        // Rule fires but returns source unchanged (full refresh)
        MaterializedViewDefinition mvDefinition = createSimpleMvDefinition();
        MaterializedViewStatus status = new MaterializedViewStatus(
                PARTIALLY_MATERIALIZED,
                ImmutableMap.of(
                        BASE_TABLE,
                        new MaterializedDataPredicates(
                                ImmutableList.of(TupleDomain.withColumnDomains(
                                        ImmutableMap.of("unmapped_col", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))),
                                ImmutableList.of("unmapped_col"))),
                Optional.empty());

        Metadata metadata = new TestingMetadataForIncrementalRefresh(
                tester().getMetadata(),
                mvDefinition,
                status);

        tester().assertThat(new IncrementalRefreshRule(metadata))
                .on(this::buildRefreshPlan)
                .matches(values("id", "ds"));
    }

    @Test
    public void testLegacyModeDisablesRule()
    {
        // Create session with legacy materialized views enabled
        FeaturesConfig legacyConfig = new FeaturesConfig()
                .setAllowLegacyMaterializedViewsToggle(true)
                .setLegacyMaterializedViews(true);

        Session tempSession = testSessionBuilder().setCatalog("local").setSchema("tiny").build();
        LocalQueryRunner legacyRunner = new LocalQueryRunner(tempSession, legacyConfig, new FunctionsConfig());

        Session legacySession = testSessionBuilder(legacyRunner.getMetadata().getSessionPropertyManager())
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("legacy_materialized_views", "true")
                .build();

        RuleTester legacyTester = new RuleTester(ImmutableList.of(), legacySession, legacyRunner, new TpchConnectorFactory(1));

        MaterializedViewStatus status = createStaleStatus();
        Metadata metadata = new TestingMetadataForIncrementalRefresh(
                legacyTester.getMetadata(),
                createMvDefinitionWithMappings(),
                status);

        // Rule should not fire because legacy mode is enabled
        legacyTester.assertThat(new IncrementalRefreshRule(metadata))
                .on(this::buildRefreshPlan)
                .doesNotFire();
    }

    private PlanNode buildRefreshPlan(PlanBuilder planBuilder)
    {
        VariableReferenceExpression idVar = planBuilder.variable("id", BIGINT);
        VariableReferenceExpression dsVar = planBuilder.variable("ds", VARCHAR);

        // Create a simple values node as the source (representing the view query)
        PlanNode source = planBuilder.values(idVar, dsVar);

        return buildRefreshMaterializedViewNode(planBuilder, source, ImmutableList.of(idVar, dsVar));
    }

    private RefreshMaterializedViewNode buildRefreshMaterializedViewNode(
            PlanBuilder planBuilder,
            PlanNode source,
            List<VariableReferenceExpression> columns)
    {
        TableHandle tableHandle = new TableHandle(
                CONNECTOR_ID,
                new TestingTableHandle(),
                TestingConnectorTransactionHandle.INSTANCE,
                Optional.empty());

        List<ColumnHandle> columnHandles = columns.stream()
                .map(col -> (ColumnHandle) new TestingColumnHandle(col.getName()))
                .collect(ImmutableList.toImmutableList());

        return new RefreshMaterializedViewNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                new SchemaTableName("test_schema", "test_mv"),
                tableHandle,
                source,
                columnHandles,
                columns);
    }

    private MaterializedViewDefinition createSimpleMvDefinition()
    {
        return new MaterializedViewDefinition(
                "SELECT id, ds FROM base_table",
                "test_schema",
                "mv_storage",
                ImmutableList.of(BASE_TABLE),
                Optional.of("test_owner"),
                Optional.of(DEFINER),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private MaterializedViewDefinition createMvDefinitionWithMappings()
    {
        // Create MV definition with proper column mappings
        // The view column is represented as a TableColumn with the MV storage table name
        TableColumn idViewCol = new TableColumn(MV_STORAGE_TABLE, "id");
        TableColumn dsViewCol = new TableColumn(MV_STORAGE_TABLE, "ds");

        TableColumn idBaseCol = new TableColumn(BASE_TABLE, "id");
        TableColumn dsBaseCol = new TableColumn(BASE_TABLE, "ds");

        ColumnMapping idMapping = new ColumnMapping(idViewCol, ImmutableList.of(idBaseCol));
        ColumnMapping dsMapping = new ColumnMapping(dsViewCol, ImmutableList.of(dsBaseCol));

        return new MaterializedViewDefinition(
                "SELECT id, ds FROM base_table",
                "test_schema",
                "mv_storage",
                ImmutableList.of(BASE_TABLE),
                Optional.of("test_owner"),
                Optional.of(DEFINER),
                ImmutableList.of(idMapping, dsMapping),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private MaterializedViewStatus createStaleStatus()
    {
        return new MaterializedViewStatus(
                PARTIALLY_MATERIALIZED,
                ImmutableMap.of(
                        BASE_TABLE,
                        new MaterializedDataPredicates(
                                ImmutableList.of(TupleDomain.withColumnDomains(
                                        ImmutableMap.of("ds", Domain.singleValue(VARCHAR, utf8Slice("2024-01-03"))))),
                                ImmutableList.of("ds"))),
                Optional.empty());
    }

    private static class TestingMetadataForIncrementalRefresh
            extends AbstractMockMetadata
    {
        private final Metadata delegate;
        private final MaterializedViewDefinition mvDefinition;
        private final MaterializedViewStatus status;

        public TestingMetadataForIncrementalRefresh(
                Metadata delegate,
                MaterializedViewDefinition mvDefinition,
                MaterializedViewStatus status)
        {
            this.delegate = delegate;
            this.mvDefinition = mvDefinition;
            this.status = status;
        }

        @Override
        public FunctionAndTypeManager getFunctionAndTypeManager()
        {
            return delegate.getFunctionAndTypeManager();
        }

        @Override
        public MetadataResolver getMetadataResolver(Session session)
        {
            return new TestingMetadataResolverForIncrementalRefresh(
                    super.getMetadataResolver(session),
                    mvDefinition,
                    status);
        }
    }

    private static class TestingMetadataWithMissingMvDefinition
            extends AbstractMockMetadata
    {
        private final Metadata delegate;

        public TestingMetadataWithMissingMvDefinition(Metadata delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public FunctionAndTypeManager getFunctionAndTypeManager()
        {
            return delegate.getFunctionAndTypeManager();
        }

        @Override
        public MetadataResolver getMetadataResolver(Session session)
        {
            return new MetadataResolver()
            {
                @Override
                public boolean catalogExists(String catalogName)
                {
                    return true;
                }

                @Override
                public boolean schemaExists(com.facebook.presto.common.CatalogSchemaName schemaName)
                {
                    return true;
                }

                @Override
                public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
                {
                    return Optional.empty();
                }

                @Override
                public List<ColumnMetadata> getColumns(TableHandle tableHandle)
                {
                    return ImmutableList.of();
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

                @Override
                public MaterializedViewStatus getMaterializedViewStatus(
                        QualifiedObjectName materializedViewName,
                        TupleDomain<String> baseQueryDomain)
                {
                    return new MaterializedViewStatus(FULLY_MATERIALIZED);
                }
            };
        }
    }

    private static class TestingMetadataResolverForIncrementalRefresh
            implements MetadataResolver
    {
        private final MetadataResolver delegate;
        private final MaterializedViewDefinition mvDefinition;
        private final MaterializedViewStatus status;

        public TestingMetadataResolverForIncrementalRefresh(
                MetadataResolver delegate,
                MaterializedViewDefinition mvDefinition,
                MaterializedViewStatus status)
        {
            this.delegate = delegate;
            this.mvDefinition = mvDefinition;
            this.status = status;
        }

        @Override
        public boolean catalogExists(String catalogName)
        {
            return delegate.catalogExists(catalogName);
        }

        @Override
        public boolean schemaExists(com.facebook.presto.common.CatalogSchemaName schemaName)
        {
            return delegate.schemaExists(schemaName);
        }

        @Override
        public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
        {
            return delegate.getTableHandle(tableName);
        }

        @Override
        public List<ColumnMetadata> getColumns(TableHandle tableHandle)
        {
            return delegate.getColumns(tableHandle);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
        {
            return delegate.getColumnHandles(tableHandle);
        }

        @Override
        public Optional<ViewDefinition> getView(QualifiedObjectName viewName)
        {
            return delegate.getView(viewName);
        }

        @Override
        public Optional<MaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName)
        {
            return Optional.of(mvDefinition);
        }

        @Override
        public MaterializedViewStatus getMaterializedViewStatus(
                QualifiedObjectName materializedViewName,
                TupleDomain<String> baseQueryDomain)
        {
            return status;
        }
    }
}
