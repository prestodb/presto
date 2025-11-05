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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.FULLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.PARTIALLY_MATERIALIZED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestMaterializedViewRewrite
        extends BaseRuleTest
{
    @Test
    public void testUseFreshDataWhenFullyMaterialized()
    {
        QualifiedObjectName materializedViewName = QualifiedObjectName.valueOf("catalog.schema.mv");

        Metadata metadata = new TestingMetadataWithMaterializedViewStatus(true);

        tester().assertThat(new MaterializedViewRewrite(metadata))
                .on(planBuilder -> {
                    VariableReferenceExpression outputA = planBuilder.variable("a", BIGINT);
                    VariableReferenceExpression dataTableA = planBuilder.variable("data_table_a", BIGINT);
                    VariableReferenceExpression viewQueryA = planBuilder.variable("view_query_a", BIGINT);

                    return planBuilder.materializedViewScan(
                            materializedViewName,
                            planBuilder.values(dataTableA),
                            planBuilder.values(viewQueryA),
                            ImmutableMap.of(outputA, dataTableA),
                            ImmutableMap.of(outputA, viewQueryA),
                            outputA);
                })
                .withSession(testSessionBuilder().setSystemProperty("legacy_materialized_views", "false").build())
                .matches(
                        project(
                                ImmutableMap.of("a", expression("data_table_a")),
                                values("data_table_a")));
    }

    @Test
    public void testUseViewQueryWhenNotFullyMaterialized()
    {
        QualifiedObjectName materializedViewName = QualifiedObjectName.valueOf("catalog.schema.mv");

        Metadata metadata = new TestingMetadataWithMaterializedViewStatus(false);

        tester().assertThat(new MaterializedViewRewrite(metadata))
                .on(planBuilder -> {
                    VariableReferenceExpression outputA = planBuilder.variable("a", BIGINT);
                    VariableReferenceExpression dataTableA = planBuilder.variable("data_table_a", BIGINT);
                    VariableReferenceExpression viewQueryA = planBuilder.variable("view_query_a", BIGINT);

                    return planBuilder.materializedViewScan(
                            materializedViewName,
                            planBuilder.values(dataTableA),
                            planBuilder.values(viewQueryA),
                            ImmutableMap.of(outputA, dataTableA),
                            ImmutableMap.of(outputA, viewQueryA),
                            outputA);
                })
                .withSession(testSessionBuilder().setSystemProperty("legacy_materialized_views", "false").build())
                .matches(
                        project(
                                ImmutableMap.of("a", expression("view_query_a")),
                                values("view_query_a")));
    }

    @Test
    public void testMultipleOutputVariables()
    {
        QualifiedObjectName materializedViewName = QualifiedObjectName.valueOf("catalog.schema.mv");

        Metadata metadata = new TestingMetadataWithMaterializedViewStatus(true);

        tester().assertThat(new MaterializedViewRewrite(metadata))
                .on(planBuilder -> {
                    VariableReferenceExpression outputA = planBuilder.variable("a", BIGINT);
                    VariableReferenceExpression outputB = planBuilder.variable("b", BIGINT);
                    VariableReferenceExpression dataTableA = planBuilder.variable("data_table_a", BIGINT);
                    VariableReferenceExpression dataTableB = planBuilder.variable("data_table_b", BIGINT);
                    VariableReferenceExpression viewQueryA = planBuilder.variable("view_query_a", BIGINT);
                    VariableReferenceExpression viewQueryB = planBuilder.variable("view_query_b", BIGINT);

                    return planBuilder.materializedViewScan(
                            materializedViewName,
                            planBuilder.values(dataTableA, dataTableB),
                            planBuilder.values(viewQueryA, viewQueryB),
                            ImmutableMap.of(outputA, dataTableA, outputB, dataTableB),
                            ImmutableMap.of(outputA, viewQueryA, outputB, viewQueryB),
                            outputA, outputB);
                })
                .withSession(testSessionBuilder().setSystemProperty("legacy_materialized_views", "false").build())
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression("data_table_a"),
                                        "b", expression("data_table_b")),
                                values("data_table_a", "data_table_b")));
    }

    private static class TestingMetadataWithMaterializedViewStatus
            extends AbstractMockMetadata
    {
        private final boolean isFullyMaterialized;

        public TestingMetadataWithMaterializedViewStatus(boolean isFullyMaterialized)
        {
            this.isFullyMaterialized = isFullyMaterialized;
        }

        @Override
        public MetadataResolver getMetadataResolver(Session session)
        {
            return new MaterializedViewTestingMetadataResolver(super.getMetadataResolver(session), isFullyMaterialized);
        }
    }

    private static class MaterializedViewTestingMetadataResolver
            implements MetadataResolver
    {
        private final MetadataResolver delegate;
        private boolean isFullyMaterialized;

        protected MaterializedViewTestingMetadataResolver(MetadataResolver delegate, boolean isFullyMaterialized)
        {
            this.delegate = delegate;
            this.isFullyMaterialized = isFullyMaterialized;
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
            return delegate.getMaterializedView(viewName);
        }

        @Override
        public MaterializedViewStatus getMaterializedViewStatus(QualifiedObjectName materializedViewName, TupleDomain<String> baseQueryDomain)
        {
            return new MaterializedViewStatus(isFullyMaterialized ? FULLY_MATERIALIZED : PARTIALLY_MATERIALIZED);
        }
    }
}
