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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TestingWriterTarget;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestVerifyNoOriginalExpression
        extends BasePlanTest
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final VariableReferenceExpression VARIABLE_REFERENCE_EXPRESSION = new VariableReferenceExpression("expr", BIGINT);
    private static final ComparisonExpression COMPARISON_EXPRESSION = new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL,
            new SymbolReference("count"),
            new Cast(new LongLiteral("5"), "bigint"));

    private Metadata metadata;
    private PlanBuilder builder;
    private ValuesNode valuesNode;
    private CallExpression comparisonCallExpression;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        builder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), metadata);
        valuesNode = builder.values();
        comparisonCallExpression = new CallExpression(
                "LESS_THAN",
                metadata.getFunctionAndTypeManager().resolveOperator(LESS_THAN, fromTypes(BIGINT, BIGINT)),
                BooleanType.BOOLEAN,
                ImmutableList.of(VARIABLE_REFERENCE_EXPRESSION, VARIABLE_REFERENCE_EXPRESSION));
    }

    @Test
    public void testValidateForJoin()
    {
        RowExpression predicate = comparisonCallExpression;
        validateJoin(predicate, null, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidateFailedForJoin()
    {
        Expression predicate = COMPARISON_EXPRESSION;
        validateJoin(null, predicate, false);
    }

    @Test
    public void testValidateForWindow()
    {
        Optional<VariableReferenceExpression> startValue = Optional.of(VARIABLE_REFERENCE_EXPRESSION);
        Optional<VariableReferenceExpression> endValue = Optional.of(VARIABLE_REFERENCE_EXPRESSION);
        Optional<String> originalStartValue = Optional.of("count");
        Optional<String> originalEndValue = Optional.of("count");
        WindowNode.Frame frame = new WindowNode.Frame(
                WindowNode.Frame.WindowType.RANGE,
                WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING,
                startValue,
                WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING,
                endValue,
                originalStartValue,
                originalEndValue);
        WindowNode.Function function = new WindowNode.Function(
                comparisonCallExpression,
                frame,
                false);
        ImmutableList<VariableReferenceExpression> partitionBy = ImmutableList.of(VARIABLE_REFERENCE_EXPRESSION);
        Optional<OrderingScheme> orderingScheme = Optional.empty();
        ImmutableMap<VariableReferenceExpression, WindowNode.Function> functions = ImmutableMap.of(VARIABLE_REFERENCE_EXPRESSION, function);
        WindowNode windowNode = builder.window(
                new WindowNode.Specification(partitionBy, orderingScheme),
                functions,
                valuesNode);
        testValidation(windowNode);
    }

    @Test
    public void testValidateSpatialJoin()
    {
        RowExpression filter = comparisonCallExpression;
        validateSpatialJoinWithFilter(filter);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidateFailedSpatialJoin()
    {
        Expression predicate = COMPARISON_EXPRESSION;
        RowExpression filter = castToRowExpression(predicate);
        validateSpatialJoinWithFilter(filter);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidateFailedCompound()
    {
        Expression predicate = COMPARISON_EXPRESSION;
        RowExpression rowExpression = castToRowExpression(predicate);
        FilterNode filterNode = builder.filter(rowExpression, valuesNode);
        ImmutableMap<VariableReferenceExpression, RowExpression> map = ImmutableMap.of(VARIABLE_REFERENCE_EXPRESSION, castToRowExpression(new SymbolReference("count")));
        ProjectNode projectNode = builder.project(new Assignments(map), filterNode);
        testValidation(projectNode);
    }

    @Test
    public void testAggregation()
    {
        ImmutableList<VariableReferenceExpression> groupingKeys = ImmutableList.of(VARIABLE_REFERENCE_EXPRESSION);
        int groupingSetCount = 1;
        ImmutableMap<VariableReferenceExpression, SortOrder> orderings = ImmutableMap.of(VARIABLE_REFERENCE_EXPRESSION, SortOrder.ASC_NULLS_FIRST);
        OrderingScheme orderingScheme = new OrderingScheme(groupingKeys.stream().map(variable -> new Ordering(variable, orderings.get(variable))).collect(toImmutableList()));
        ImmutableMap<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = ImmutableMap.of(
                VARIABLE_REFERENCE_EXPRESSION,
                new AggregationNode.Aggregation(
                        comparisonCallExpression,
                        Optional.of(comparisonCallExpression),
                        Optional.of(orderingScheme),
                        false,
                        Optional.of(new VariableReferenceExpression("orderkey", BIGINT))));
        ImmutableSet<Integer> globalGroupingSets = ImmutableSet.of(1);
        AggregationNode.GroupingSetDescriptor groupingSets = new AggregationNode.GroupingSetDescriptor(groupingKeys, groupingSetCount, globalGroupingSets);
        ImmutableList<VariableReferenceExpression> preGroupedVariables = ImmutableList.of();
        Optional<VariableReferenceExpression> hashVariable = Optional.of(VARIABLE_REFERENCE_EXPRESSION);
        Optional<VariableReferenceExpression> groupIdVariable = Optional.of(VARIABLE_REFERENCE_EXPRESSION);
        AggregationNode aggregationNode = new AggregationNode(
                new PlanNodeId("1"),
                valuesNode,
                aggregations,
                groupingSets,
                preGroupedVariables,
                AggregationNode.Step.SINGLE,
                hashVariable,
                groupIdVariable);
        testValidation(aggregationNode);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidateForApplyFailed()
    {
        ImmutableMap<VariableReferenceExpression, RowExpression> map = ImmutableMap.of(VARIABLE_REFERENCE_EXPRESSION, castToRowExpression(new SymbolReference("count")));
        Assignments assignments = new Assignments(map);
        ImmutableList<VariableReferenceExpression> variableReferenceExpressions = ImmutableList.of(VARIABLE_REFERENCE_EXPRESSION);
        ApplyNode applyNode = builder.apply(
                assignments,
                variableReferenceExpressions,
                valuesNode,
                valuesNode);
        testValidation(applyNode);
    }

    @Test
    public void testTableFinish()
    {
        TableFinishNode tableFinishNode = new TableFinishNode(
                new PlanNodeId("1"),
                valuesNode,
                Optional.of(new TestingWriterTarget()),
                VARIABLE_REFERENCE_EXPRESSION,
                Optional.empty(),
                Optional.empty());
        testValidation(tableFinishNode);
    }

    @Test
    public void testTableWriter()
    {
        ImmutableList<VariableReferenceExpression> variableReferenceExpressions = ImmutableList.of(VARIABLE_REFERENCE_EXPRESSION);
        TableWriterNode tableWriterNode = builder.tableWriter(
                variableReferenceExpressions,
                ImmutableList.of(""),
                valuesNode);
        testValidation(tableWriterNode);
    }

    private void validateJoin(RowExpression rowExpressionPredicate, Expression expressionPredicate, boolean ifRowExpression)
    {
        ImmutableMap<VariableReferenceExpression, RowExpression> map = ImmutableMap.of(
                VARIABLE_REFERENCE_EXPRESSION,
                VARIABLE_REFERENCE_EXPRESSION);
        ProjectNode projectNode = builder.project(new Assignments(map), valuesNode);
        JoinNode joinNode;
        if (ifRowExpression) {
            joinNode = builder.join(
                    JoinNode.Type.INNER,
                    projectNode,
                    projectNode,
                    rowExpressionPredicate);
        }
        else {
            joinNode = builder.join(
                    JoinNode.Type.INNER,
                    projectNode,
                    projectNode,
                    castToRowExpression(expressionPredicate));
        }
        testValidation(joinNode);
    }

    private void validateSpatialJoinWithFilter(RowExpression filter)
    {
        ImmutableList<VariableReferenceExpression> outputVariables = ImmutableList.of(VARIABLE_REFERENCE_EXPRESSION);
        Optional<VariableReferenceExpression> leftPartitionVariable = Optional.of(VARIABLE_REFERENCE_EXPRESSION);
        Optional<VariableReferenceExpression> rightPartitionVariable = Optional.of(VARIABLE_REFERENCE_EXPRESSION);
        Optional<String> kdbTree = Optional.of("");
        ImmutableMap<VariableReferenceExpression, RowExpression> map = ImmutableMap.of(
                VARIABLE_REFERENCE_EXPRESSION,
                VARIABLE_REFERENCE_EXPRESSION);
        ProjectNode projectNode = builder.project(new Assignments(map), valuesNode);
        SpatialJoinNode spatialJoinNode = new SpatialJoinNode(
                new PlanNodeId("1"),
                SpatialJoinNode.Type.INNER,
                projectNode,
                projectNode,
                outputVariables,
                filter,
                leftPartitionVariable,
                rightPartitionVariable,
                kdbTree);
        testValidation(spatialJoinNode);
    }

    private void testValidation(PlanNode node)
    {
        getQueryRunner().inTransaction(session -> {
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new VerifyNoOriginalExpression().validate(node, session, metadata, SQL_PARSER, builder.getTypes(), WarningCollector.NOOP);
            return null;
        });
    }
}
