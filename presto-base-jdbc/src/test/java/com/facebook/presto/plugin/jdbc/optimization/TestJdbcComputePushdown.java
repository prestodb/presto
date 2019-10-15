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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestJdbcComputePushdown
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
    private static final PlanNodeIdAllocator ID_ALLOCATOR = new PlanNodeIdAllocator();

    private final JdbcComputePushdown jdbcComputePushdown;

    public TestJdbcComputePushdown()
    {
        this.jdbcComputePushdown = new JdbcComputePushdown(null, null, null, null);
    }

    @Test
    public void testJdbcComputePushdownIsNoop()
    {
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle("cat1", new SchemaTableName("schema", "table"), null, null, "table");
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.none());
        TableHandle tableHandle = new TableHandle(new ConnectorId("Jdbc"), jdbcTableHandle, new ConnectorTransactionHandle() {}, Optional.of(jdbcTableLayoutHandle));
        PlanNode original = filter(tableScan(tableHandle, "a", "b"), TRUE_CONSTANT);

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                TRUE_CONSTANT.toString(),
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle)));
    }

    private static VariableReferenceExpression newBigintVariable(String name)
    {
        return new VariableReferenceExpression(name, BIGINT);
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected)
    {
        assertPlanMatch(actual, expected, TypeProvider.empty());
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected, TypeProvider typeProvider)
    {
        PlanAssert.assertPlan(
                TEST_SESSION,
                METADATA,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, typeProvider, StatsAndCosts.empty()),
                expected);
    }

    private TableScanNode tableScan(TableHandle tableHandle, String... columnNames)
    {
        return PLAN_BUILDER.tableScan(
                tableHandle,
                Arrays.stream(columnNames).map(TestJdbcComputePushdown::newBigintVariable).collect(toImmutableList()),
                Arrays.stream(columnNames).map(TestJdbcComputePushdown::newBigintVariable).collect(toMap(identity(), variable -> new ColumnHandle() {})));
    }

    private FilterNode filter(PlanNode source, RowExpression predicate)
    {
        return PLAN_BUILDER.filter(predicate, source);
    }

    private static final class JdbcTableScanMatcher
            implements Matcher
    {
        private final JdbcTableLayoutHandle jdbcTableLayoutHandle;

        static PlanMatchPattern jdbcTableScanPattern(JdbcTableLayoutHandle jdbcTableLayoutHandle)
        {
            return node(TableScanNode.class).with(new JdbcTableScanMatcher(jdbcTableLayoutHandle));
        }

        private JdbcTableScanMatcher(JdbcTableLayoutHandle jdbcTableLayoutHandle)
        {
            this.jdbcTableLayoutHandle = jdbcTableLayoutHandle;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

            TableScanNode tableScanNode = (TableScanNode) node;
            JdbcTableLayoutHandle layoutHandle = (JdbcTableLayoutHandle) tableScanNode.getTable().getLayout().get();
            if (jdbcTableLayoutHandle.getTable().equals(layoutHandle.getTable()) && jdbcTableLayoutHandle.getTupleDomain().equals(layoutHandle.getTupleDomain())) {
                return MatchResult.match();
            }

            return MatchResult.NO_MATCH;
        }
    }
}
