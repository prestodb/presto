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
package com.facebook.presto.pinot.query;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotPlanOptimizer;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.TestPinotQueryBase;
import com.facebook.presto.pinot.TestPinotSplitManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestPinotPlanOptimizer
        extends TestPinotQueryBase
{
    private final LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
            new RowExpressionDeterminismEvaluator(functionAndTypeManager),
            new FunctionResolution(functionAndTypeManager),
            functionAndTypeManager);
    protected final PinotTableHandle pinotTable = TestPinotSplitManager.hybridTable;
    protected final SessionHolder defaultSessionHolder = getDefaultSessionHolder();

    public SessionHolder getDefaultSessionHolder()
    {
        return new SessionHolder(false, useSqlSyntax());
    }

    public boolean useSqlSyntax()
    {
        return false;
    }

    protected void assertPlanMatch(PlanNode actual, PlanMatchPattern expected, TypeProvider typeProvider)
    {
        PlanAssert.assertPlan(
                defaultSessionHolder.getSession(),
                metadata,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, typeProvider, StatsAndCosts.empty()),
                expected);
    }

    static final class PinotTableScanMatcher
            implements Matcher
    {
        private final ConnectorId connectorId;
        private final String tableName;
        private final Optional<String> pinotQueryRegex;
        private final Optional<Boolean> scanParallelismExpected;
        private final String[] columns;
        private final boolean useSqlSyntax;

        static PlanMatchPattern match(
                String connectorName,
                String tableName,
                Optional<String> pinotQueryRegex,
                Optional<Boolean> scanParallelismExpected,
                boolean useSqlSyntax,
                String... columnNames)
        {
            return node(TableScanNode.class)
                    .with(new PinotTableScanMatcher(
                            new ConnectorId(connectorName),
                            tableName,
                            pinotQueryRegex,
                            scanParallelismExpected,
                            useSqlSyntax,
                            columnNames));
        }

        static PlanMatchPattern match(
                PinotTableHandle tableHandle,
                Optional<String> pinotQueryRegex,
                Optional<Boolean> scanParallelismExpected,
                List<VariableReferenceExpression> variables,
                boolean useSqlSyntax)
        {
            return match(
                    tableHandle.getConnectorId(),
                    tableHandle.getTableName(),
                    pinotQueryRegex,
                    scanParallelismExpected,
                    useSqlSyntax,
                    variables.stream().map(VariableReferenceExpression::getName).toArray(String[]::new));
        }

        private PinotTableScanMatcher(
                ConnectorId connectorId,
                String tableName,
                Optional<String> pinotQueryRegex,
                Optional<Boolean> scanParallelismExpected,
                boolean useSqlSyntax,
                String... columns)
        {
            this.connectorId = connectorId;
            this.pinotQueryRegex = pinotQueryRegex;
            this.scanParallelismExpected = scanParallelismExpected;
            this.columns = columns;
            this.tableName = tableName;
            this.useSqlSyntax = useSqlSyntax;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        private static boolean checkPinotQueryMatches(Optional<String> regex, Optional<String> pql)
        {
            if (!pql.isPresent() && !regex.isPresent()) {
                return true;
            }
            if (pql.isPresent() && regex.isPresent()) {
                String toMatch = pql.get();
                Pattern compiled = Pattern.compile(regex.get(), Pattern.CASE_INSENSITIVE);
                return compiled.matcher(toMatch).matches();
            }
            return false;
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
            if (connectorId.equals(tableScanNode.getTable().getConnectorId())) {
                PinotTableHandle pinotTableHandle = (PinotTableHandle) tableScanNode.getTable().getConnectorHandle();
                if (pinotTableHandle.getTableName().equals(tableName)) {
                    Optional<String> pinotQuery = pinotTableHandle.getPinotQuery().map(PinotQueryGenerator.GeneratedPinotQuery::getQuery);
                    if (checkPinotQueryMatches(pinotQueryRegex, pinotQuery)) {
                        return MatchResult.match(SymbolAliases.builder().putAll(Arrays.stream(columns).collect(toMap(identity(), SymbolReference::new))).build());
                    }
                }
            }
            return MatchResult.NO_MATCH;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("connectorId", connectorId)
                    .add("tableName", tableName)
                    .add("pinotQueryRegex", pinotQueryRegex)
                    .add("scanParallelismExpected", scanParallelismExpected)
                    .add("columns", columns)
                    .toString();
        }
    }

    @Test
    public void testLimitPushdownWithStarSelection()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        PlanNode originalPlan = limit(planBuilder, 50L, tableScan(planBuilder, pinotTable, regionId, city, fare, secondsSinceEpoch));
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(optimized, PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, secondsSinceEpoch FROM hybrid LIMIT 50"), Optional.of(false), originalPlan.getOutputVariables(), useSqlSyntax()), typeProvider);
    }

    @Test
    public void testPartialPredicatePushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, regionId, city, fare, secondsSinceEpoch);
        FilterNode filter = filter(planBuilder, tableScanNode, getRowExpression("lower(substr(city, 0, 3)) = 'del' AND fare > 100", defaultSessionHolder));
        PlanNode originalPlan = limit(planBuilder, 50L, filter);
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        PlanMatchPattern tableScanMatcher = PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, secondsSinceEpoch FROM hybrid__TABLE_NAME_SUFFIX_TEMPLATE__ WHERE \\(fare > 100\\).*"), Optional.of(true), filter.getOutputVariables(), useSqlSyntax());
        assertPlanMatch(optimized, PlanMatchPattern.limit(50L, PlanMatchPattern.filter("lower(substr(city, 0, 3)) = 'del'", tableScanMatcher)), typeProvider);
    }

    @Test
    public void testDatePredicatePushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        FilterNode filter = filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, city, fare, daysSinceEpoch), getRowExpression("dayssinceepoch < DATE '2014-01-31'", defaultSessionHolder));
        PlanNode originalPlan = limit(planBuilder, 50L, filter);
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(optimized, PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, daysSinceEpoch FROM hybrid WHERE \\(daysSinceEpoch < 16101\\) LIMIT 50"), Optional.of(false), originalPlan.getOutputVariables(), useSqlSyntax()), typeProvider);
    }

    @Test
    public void testDateCastingPredicatePushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        FilterNode filter = filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, city, fare, daysSinceEpoch), getRowExpression("cast(dayssinceepoch as timestamp) < TIMESTAMP '2014-01-31 00:00:00 UTC'", defaultSessionHolder));
        PlanNode originalPlan = limit(planBuilder, 50L, filter);
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(optimized, PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, daysSinceEpoch FROM hybrid WHERE \\(daysSinceEpoch < 16101\\) LIMIT 50"), Optional.of(false), originalPlan.getOutputVariables(), useSqlSyntax()), typeProvider);
    }

    @Test
    public void testTimestampPredicatePushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        FilterNode filter = filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, city, fare, millisSinceEpoch), getRowExpression("millissinceepoch < TIMESTAMP '2014-01-31 00:00:00 UTC'", defaultSessionHolder));
        PlanNode originalPlan = limit(planBuilder, 50L, filter);
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(optimized, PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, millisSinceEpoch FROM hybrid WHERE \\(millisSinceEpoch < 1391126400000\\) LIMIT 50"), Optional.of(false), originalPlan.getOutputVariables(), useSqlSyntax()), typeProvider);
    }

    @Test
    public void testTimestampCastingPredicatePushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        FilterNode filter = filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, city, fare, millisSinceEpoch), getRowExpression("cast(millissinceepoch as date) < DATE '2014-01-31'", defaultSessionHolder));
        PlanNode originalPlan = limit(planBuilder, 50L, filter);
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(optimized, PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, millisSinceEpoch FROM hybrid WHERE \\(millisSinceEpoch < 1391126400000\\) LIMIT 50"), Optional.of(false), originalPlan.getOutputVariables(), useSqlSyntax()), typeProvider);
    }

    @Test
    public void testDateFieldCompareToTimestampLiteralPredicatePushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        FilterNode filter = filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, city, fare, daysSinceEpoch), getRowExpression("dayssinceepoch <  TIMESTAMP '2014-01-31 00:00:00 UTC'", defaultSessionHolder));
        PlanNode originalPlan = limit(planBuilder, 50L, filter);
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(optimized, PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, daysSinceEpoch FROM hybrid WHERE \\(dayssinceepoch < 16101\\) LIMIT 50"), Optional.of(false), originalPlan.getOutputVariables(), useSqlSyntax()), typeProvider);
    }

    @Test
    public void testTimestampFieldCompareToDateLiteralPredicatePushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        FilterNode filter = filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, city, fare, millisSinceEpoch), getRowExpression("millissinceepoch <  DATE '2014-01-31'", defaultSessionHolder));
        PlanNode originalPlan = limit(planBuilder, 50L, filter);
        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);
        assertPlanMatch(optimized, PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, millisSinceEpoch FROM hybrid WHERE \\(millisSinceEpoch < 1391126400000\\) LIMIT 50"), Optional.of(false), originalPlan.getOutputVariables(), useSqlSyntax()), typeProvider);
    }

    @Test
    public void testUnsupportedPredicatePushdown()
    {
        Map<String, ExpectedValueProvider<FunctionCall>> aggregationsSecond = ImmutableMap.of("count", PlanMatchPattern.functionCall("count", false, ImmutableList.of()));

        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        PlanNode limit = limit(planBuilder, 50L, tableScan(planBuilder, pinotTable, regionId, city, fare, secondsSinceEpoch));
        PlanNode originalPlan = planBuilder.aggregation(builder -> builder.source(limit).globalGrouping().addAggregation(new VariableReferenceExpression("count", BIGINT), getRowExpression("count(*)", defaultSessionHolder)));

        PlanNode optimized = getOptimizedPlan(planBuilder, originalPlan);

        PlanMatchPattern tableScanMatcher = PinotTableScanMatcher.match(pinotTable, Optional.of("SELECT regionId, city, fare, secondsSinceEpoch FROM hybrid LIMIT 50"), Optional.of(false), originalPlan.getOutputVariables(), useSqlSyntax());
        assertPlanMatch(optimized, aggregation(aggregationsSecond, tableScanMatcher), typeProvider);
    }

    protected PlanNode getOptimizedPlan(PlanBuilder planBuilder, PlanNode originalPlan)
    {
        PinotConfig pinotConfig = new PinotConfig();
        PinotQueryGenerator pinotQueryGenerator = new PinotQueryGenerator(pinotConfig, functionAndTypeManager, functionAndTypeManager, standardFunctionResolution);
        PinotPlanOptimizer optimizer = new PinotPlanOptimizer(pinotQueryGenerator, functionAndTypeManager, functionAndTypeManager, logicalRowExpressions, standardFunctionResolution);
        return optimizer.optimize(originalPlan, defaultSessionHolder.getConnectorSession(), new PlanVariableAllocator(), planBuilder.getIdAllocator());
    }

    @Test
    public void testDistinctCountInSubQueryPushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        Map<VariableReferenceExpression, PinotColumnHandle> leftColumnHandleMap = ImmutableMap.of(new VariableReferenceExpression("regionid", regionId.getDataType()), regionId);
        PlanNode leftJustScan = tableScan(planBuilder, pinotTable, leftColumnHandleMap);
        PlanNode leftMarkDistinct = markDistinct(planBuilder, variable("regionid$distinct"), ImmutableList.of(variable("regionid")), leftJustScan);
        PlanNode leftAggregation = planBuilder.aggregation(aggBuilder -> aggBuilder.source(leftMarkDistinct).addAggregation(planBuilder.variable("count(regionid)"), getRowExpression("count(regionid)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct"))).globalGrouping());
        PlanNode optimized = getOptimizedPlan(planBuilder, leftAggregation);
        assertPlanMatch(
                optimized,
                PinotTableScanMatcher.match(
                        pinotTable,
                        Optional.of("SELECT DISTINCTCOUNT\\(regionId\\) FROM hybrid"),
                        Optional.of(false),
                        leftAggregation.getOutputVariables(),
                        useSqlSyntax()),
                typeProvider);

        Map<VariableReferenceExpression, PinotColumnHandle> rightColumnHandleMap = ImmutableMap.of(new VariableReferenceExpression("regionid_33", regionId.getDataType()), regionId);
        PlanNode rightJustScan = tableScan(planBuilder, pinotTable, rightColumnHandleMap);
        PlanNode rightMarkDistinct = markDistinct(planBuilder, variable("regionid$distinct_62"), ImmutableList.of(variable("regionid")), rightJustScan);
        PlanNode rightAggregation = planBuilder.aggregation(aggBuilder -> aggBuilder.source(rightMarkDistinct).addAggregation(planBuilder.variable("count(regionid_33)"), getRowExpression("count(regionid_33)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct_62"))).globalGrouping());

        optimized = getOptimizedPlan(planBuilder, rightAggregation);
        assertPlanMatch(
                optimized,
                PinotTableScanMatcher.match(
                        pinotTable,
                        Optional.of("SELECT DISTINCTCOUNT\\(regionId\\) FROM hybrid"),
                        Optional.of(false),
                        rightAggregation.getOutputVariables(),
                        useSqlSyntax()),
                typeProvider);
    }

    @Test
    public void testSetOperationQueryWithSubQueriesPushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        Map<VariableReferenceExpression, PinotColumnHandle> leftColumnHandleMap = ImmutableMap.of(new VariableReferenceExpression("regionid", regionId.getDataType()), regionId);
        PlanNode leftJustScan = tableScan(planBuilder, pinotTable, leftColumnHandleMap);
        PlanNode leftMarkDistinct = markDistinct(planBuilder, variable("regionid$distinct"), ImmutableList.of(variable("regionid")), leftJustScan);
        PlanNode leftAggregation = planBuilder.aggregation(aggBuilder -> aggBuilder.source(leftMarkDistinct).addAggregation(planBuilder.variable("count(regionid)"), getRowExpression("count(regionid)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct"))).globalGrouping());

        Map<VariableReferenceExpression, PinotColumnHandle> rightColumnHandleMap = ImmutableMap.of(new VariableReferenceExpression("regionid_33", regionId.getDataType()), regionId);
        PlanNode rightJustScan = tableScan(planBuilder, pinotTable, rightColumnHandleMap);
        PlanNode rightMarkDistinct = markDistinct(planBuilder, variable("regionid$distinct_62"), ImmutableList.of(variable("regionid")), rightJustScan);
        PlanNode rightAggregation = planBuilder.aggregation(aggBuilder -> aggBuilder.source(rightMarkDistinct).addAggregation(planBuilder.variable("count(regionid_33)"), getRowExpression("count(regionid_33)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct_62"))).globalGrouping());

        validateSetOperationOptimizer(planBuilder, planBuilder.union(ArrayListMultimap.create(), ImmutableList.of(leftAggregation, rightAggregation)));
        validateSetOperationOptimizer(planBuilder, planBuilder.intersect(ArrayListMultimap.create(), ImmutableList.of(leftAggregation, rightAggregation)));
        validateSetOperationOptimizer(planBuilder, planBuilder.except(ArrayListMultimap.create(), ImmutableList.of(leftAggregation, rightAggregation)));
    }

    private void validateSetOperationOptimizer(PlanBuilder planBuilder, PlanNode setOperationPlanNode)
    {
        for (PlanNode source : getOptimizedPlan(planBuilder, setOperationPlanNode).getSources()) {
            assertPlanMatch(
                    source,
                    PinotTableScanMatcher.match(
                            pinotTable,
                            Optional.of("SELECT DISTINCTCOUNT\\(regionId\\) FROM hybrid"),
                            Optional.of(false),
                            source.getOutputVariables(),
                            useSqlSyntax()),
                    typeProvider);
        }
    }
}
