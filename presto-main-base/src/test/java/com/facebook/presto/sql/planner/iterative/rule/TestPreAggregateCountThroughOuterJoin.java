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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.optimizations.PredicatePushDown.createDynamicFilterExpression;

public class TestPreAggregateCountThroughOuterJoin
        extends BaseRuleTest
{
    @Test
    public void testPreAggregatesInnerCountBelowLeftJoin()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression outerValue = p.variable("outer_value", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey, outerValue),
                                    p.values(innerKey, innerValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .matches(node(ProjectNode.class,
                        node(AggregationNode.class,
                                node(JoinNode.class,
                                        any(),
                                        node(AggregationNode.class, any())))));
    }

    @Test
    public void testPreAggregatesInnerCountBelowRightJoin()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .source(p.join(
                                    RIGHT,
                                    p.values(innerKey, innerValue),
                                    p.values(outerKey),
                                    new EquiJoinClause(innerKey, outerKey))));
                })
                .matches(node(ProjectNode.class,
                        node(AggregationNode.class,
                                node(JoinNode.class,
                                        node(AggregationNode.class, any()),
                                        any()))));
    }

    @Test
    public void testPreservesDynamicFilters()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .source(p.join(
                                    RIGHT,
                                    p.filter(
                                            createDynamicFilterExpression("DF", innerKey, getMetadata().getFunctionAndTypeManager()),
                                            p.values(innerKey, innerValue)),
                                    p.values(outerKey),
                                    ImmutableList.of(new EquiJoinClause(innerKey, outerKey)),
                                    ImmutableList.of(innerKey, innerValue, outerKey),
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty(),
                                    ImmutableMap.of("DF", outerKey))));
                })
                .matches(node(ProjectNode.class,
                        node(AggregationNode.class,
                                node(JoinNode.class,
                                        node(AggregationNode.class, any()),
                                        any())
                                        .with(new JoinDynamicFiltersMatcher("DF", "outer_key")))));
    }

    @Test
    public void testPreAggregatesMultipleInnerCounts()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression innerOtherValue = p.variable("inner_other_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);
                    VariableReferenceExpression otherCount = p.variable("other_count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .addAggregation(otherCount, p.rowExpression("count(inner_other_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.values(innerKey, innerValue, innerOtherValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .matches(node(ProjectNode.class,
                        node(AggregationNode.class,
                                node(JoinNode.class,
                                        any(),
                                        node(AggregationNode.class, any())))));
    }

    @Test
    public void testUsesCountAllForTrustedNotNullInnerColumn()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);
                    ColumnHandle innerKeyColumn = new TestingColumnHandle("inner_key");
                    ColumnHandle innerValueColumn = new TestingColumnHandle("inner_value");
                    TableHandle innerTable = new TableHandle(
                            new ConnectorId("testConnector"),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.tableScan(
                                            innerTable,
                                            ImmutableList.of(innerKey, innerValue),
                                            ImmutableMap.of(innerKey, innerKeyColumn, innerValue, innerValueColumn),
                                            TupleDomain.all(),
                                            TupleDomain.all(),
                                            ImmutableList.of(new NotNullConstraint<>(innerValueColumn))),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .matches(node(ProjectNode.class,
                        node(AggregationNode.class,
                                node(JoinNode.class,
                                        any(),
                                        aggregation(
                                                ImmutableMap.of("pre_count", functionCall("count", false, ImmutableList.of())),
                                                any())))));
    }

    @Test
    public void testUsesCountAllForTrustedNotNullInnerColumnThroughProjection()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression projectedInnerValue = p.variable("projected_inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);
                    ColumnHandle innerKeyColumn = new TestingColumnHandle("inner_key");
                    ColumnHandle innerValueColumn = new TestingColumnHandle("inner_value");
                    TableHandle innerTable = new TableHandle(
                            new ConnectorId("testConnector"),
                            new TestingTableHandle(),
                            TestingTransactionHandle.create(),
                            Optional.empty());

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(projected_inner_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.project(
                                            Assignments.builder()
                                                    .put(innerKey, innerKey)
                                                    .put(projectedInnerValue, innerValue)
                                                    .build(),
                                            p.tableScan(
                                                    innerTable,
                                                    ImmutableList.of(innerKey, innerValue),
                                                    ImmutableMap.of(innerKey, innerKeyColumn, innerValue, innerValueColumn),
                                                    TupleDomain.all(),
                                                    TupleDomain.all(),
                                                    ImmutableList.of(new NotNullConstraint<>(innerValueColumn)))),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .matches(node(ProjectNode.class,
                        node(AggregationNode.class,
                                node(JoinNode.class,
                                        any(),
                                        aggregation(
                                                ImmutableMap.of("pre_count", functionCall("count", false, ImmutableList.of())),
                                                any())))));
    }

    @Test
    public void testPreAggregatesGlobalCount()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .globalGrouping()
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.values(innerKey, innerValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .matches(node(ProjectNode.class,
                        node(AggregationNode.class,
                                node(JoinNode.class,
                                        any(),
                                        node(AggregationNode.class, any())))));
    }

    @Test
    public void testDoesNotFireForOuterSideCount()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression outerValue = p.variable("outer_value", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(outer_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey, outerValue),
                                    p.values(innerKey, innerValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForConstantCount()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(1)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.values(innerKey),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForCountAll()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(*)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.values(innerKey),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForJoinWithHashVariables()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression outerHash = p.variable("outer_hash", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression innerHash = p.variable("inner_hash", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey, outerHash),
                                    p.values(innerKey, innerValue, innerHash),
                                    ImmutableList.of(new EquiJoinClause(outerKey, innerKey)),
                                    ImmutableList.of(outerKey, outerHash, innerKey, innerValue, innerHash),
                                    Optional.empty(),
                                    Optional.of(outerHash),
                                    Optional.of(innerHash))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForExpressionCount()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(coalesce(inner_value, BIGINT '1'))"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.values(innerKey, innerValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNullableExpressionCount()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(nullif(inner_value, BIGINT '0'))"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.values(innerKey, innerValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNondeterministicExpressionCount()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(random())"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.values(innerKey),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForMixedOuterAndInnerExpressionCount()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression outerValue = p.variable("outer_value", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(outer_value + inner_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey, outerValue),
                                    p.values(innerKey, innerValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenExistingDistinctOuterJoinRuleCanApplyThroughIdentityProjection()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression unique = p.variable("unique", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey, unique)
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.project(
                                            Assignments.builder()
                                                    .put(outerKey, outerKey)
                                                    .put(unique, unique)
                                                    .build(),
                                            p.assignUniqueId(unique, p.values(outerKey))),
                                    p.values(innerKey, innerValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PreAggregateCountThroughOuterJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "false")
                .on(p -> {
                    VariableReferenceExpression outerKey = p.variable("outer_key", BIGINT);
                    VariableReferenceExpression innerKey = p.variable("inner_key", BIGINT);
                    VariableReferenceExpression innerValue = p.variable("inner_value", BIGINT);
                    VariableReferenceExpression count = p.variable("count", BIGINT);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(outerKey)
                            .addAggregation(count, p.rowExpression("count(inner_value)"))
                            .source(p.join(
                                    LEFT,
                                    p.values(outerKey),
                                    p.values(innerKey, innerValue),
                                    new EquiJoinClause(outerKey, innerKey))));
                })
                .doesNotFire();
    }

    private static class JoinDynamicFiltersMatcher
            implements Matcher
    {
        private final String dynamicFilterId;
        private final String buildVariableName;

        private JoinDynamicFiltersMatcher(String dynamicFilterId, String buildVariableName)
        {
            this.dynamicFilterId = dynamicFilterId;
            this.buildVariableName = buildVariableName;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof JoinNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            JoinNode join = (JoinNode) node;
            VariableReferenceExpression buildVariable = join.getDynamicFilters().get(dynamicFilterId);
            return new MatchResult(join.getDynamicFilters().size() == 1
                    && buildVariable != null
                    && buildVariable.getName().equals(buildVariableName));
        }
    }
}
