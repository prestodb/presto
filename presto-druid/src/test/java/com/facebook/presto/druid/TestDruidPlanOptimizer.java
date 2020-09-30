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
package com.facebook.presto.druid;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestDruidPlanOptimizer
        extends TestDruidQueryBase
{
    private final DruidTableHandle druidTableOne = realtimeOnlyTable;
    private final DruidTableHandle druidTableTwo = hybridTable;
    private final SessionHolder defaultSessionHolder = new SessionHolder();

    static final class DruidTableScanMatcher
            implements Matcher
    {
        private final String tableName;
        private final String expectedDql;

        static PlanMatchPattern match(
                String tableName,
                String expectedDql)
        {
            return node(TableScanNode.class)
                    .with(new DruidTableScanMatcher(
                            tableName,
                            expectedDql));
        }

        private DruidTableScanMatcher(
                String tableName,
                String expectedDql)
        {
            this.tableName = tableName;
            this.expectedDql = expectedDql;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(
                PlanNode node,
                StatsProvider stats,
                Session session,
                Metadata metadata,
                SymbolAliases symbolAliases)
        {
            checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
            TableScanNode tableScanNode = (TableScanNode) node;
            DruidTableHandle druidTableHandle = (DruidTableHandle) tableScanNode.getTable().getConnectorHandle();
            if (druidTableHandle.getTableName().equals(tableName)) {
                Optional<String> actualDql = druidTableHandle.getDql().map(DruidQueryGenerator.GeneratedDql::getDql);
                return actualDql.isPresent() && actualDql.get().equalsIgnoreCase(expectedDql) ? MatchResult.match() : MatchResult.NO_MATCH;
            }
            return MatchResult.NO_MATCH;
        }
    }

    @Test
    public void testUnionAll()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();

        AggregationNode aggregationOne = simpleAggregationSum(planBuilder, tableScan(planBuilder, druidTableOne, city, fare), variableAllocator, ImmutableList.of(city), fare);
        AggregationNode aggregationTwo = simpleAggregationSum(planBuilder, tableScan(planBuilder, druidTableTwo, city, fare), variableAllocator, ImmutableList.of(city), fare);
        VariableReferenceExpression groupByColumn = variableAllocator.newVariable(city.getColumnName(), city.getColumnType());
        VariableReferenceExpression sumColumn = variableAllocator.newVariable(fare.getColumnName(), fare.getColumnType());
        PlanNode originalPlan = new UnionNode(planBuilder.getIdAllocator().getNextId(),
                ImmutableList.of(aggregationOne, aggregationTwo),
                ImmutableList.of(groupByColumn, sumColumn),
                ImmutableMap.of(
                        groupByColumn,
                        Stream.concat(aggregationOne.getGroupingKeys().stream(), aggregationTwo.getGroupingKeys().stream()).collect(toImmutableList()),
                        sumColumn,
                        ImmutableList.of(Iterables.getOnlyElement(aggregationOne.getAggregations().keySet()), Iterables.getOnlyElement(aggregationTwo.getAggregations().keySet()))));
        PlanNode optimizedPlan = getOptimizedPlan(planBuilder, originalPlan);
        PlanMatchPattern tableScanMatcherOne = DruidTableScanMatcher.match(druidTableOne.getTableName(), "SELECT \"city\", sum(fare) FROM \"realtimeOnly\" GROUP BY \"city\"");
        PlanMatchPattern tableScanMatcherTwo = DruidTableScanMatcher.match(druidTableTwo.getTableName(), "SELECT \"city\", sum(fare) FROM \"hybrid\" GROUP BY \"city\"");
        assertPlanMatch(optimizedPlan, PlanMatchPattern.union(tableScanMatcherOne, tableScanMatcherTwo), typeProvider);
    }

    private void assertPlanMatch(PlanNode actual, PlanMatchPattern expected, TypeProvider typeProvider)
    {
        PlanAssert.assertPlan(
                defaultSessionHolder.getSession(),
                metadata,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, typeProvider, StatsAndCosts.empty()),
                expected);
    }

    private PlanNode getOptimizedPlan(PlanBuilder planBuilder, PlanNode originalPlan)
    {
        DruidQueryGenerator druidQueryGenerator = new DruidQueryGenerator(functionAndTypeManager, functionAndTypeManager, standardFunctionResolution);
        DruidPlanOptimizer optimizer = new DruidPlanOptimizer(druidQueryGenerator, functionAndTypeManager, new RowExpressionDeterminismEvaluator(functionAndTypeManager), functionAndTypeManager, standardFunctionResolution);
        return optimizer.optimize(originalPlan, defaultSessionHolder.getConnectorSession(), new PlanVariableAllocator(), planBuilder.getIdAllocator());
    }

    private AggregationNode simpleAggregationSum(PlanBuilder pb, PlanNode source, PlanVariableAllocator variableAllocator, List<DruidColumnHandle> groupByColumns, DruidColumnHandle sumColumn)
    {
        return new AggregationNode(
                pb.getIdAllocator().getNextId(),
                source,
                ImmutableMap.of(variableAllocator.newVariable("sum", sumColumn.getColumnType()), new AggregationNode.Aggregation(
                        new CallExpression("sum",
                                functionAndTypeManager.lookupFunction("sum", fromTypes(sumColumn.getColumnType())),
                                sumColumn.getColumnType(),
                                source.getOutputVariables().stream().filter(col -> col.getName().startsWith(sumColumn.getColumnName())).collect(toImmutableList())),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                singleGroupingSet(source.getOutputVariables().stream().filter(col -> {
                    for (DruidColumnHandle druidColumnHandle : groupByColumns) {
                        if (col.getName().startsWith(druidColumnHandle.getColumnName())) {
                            return true;
                        }
                    }
                    return false;
                }).collect(toImmutableList())),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
    }
}
