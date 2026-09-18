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

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.LEGACY_UNNEST;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_JOIN_FAN_OUT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCollapseFanoutJoinWithArrayAggUnnest
        extends BaseRuleTest
{
    @Test
    public void testFiresCollapsingBuildSideOfInnerJoin()
    {
        // a JOIN (SELECT k1, k2, sum(v) measure FROM t GROUP BY k1, k2) b ON a.k1 = b.k1
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(AggregationNode.class,
                                                                        node(ValuesNode.class))))))));
    }

    @Test
    public void testFiresWithJoinFilterOnPreservedColumn()
    {
        // a JOIN (SELECT k1, k2, sum(v) measure FROM t GROUP BY k1, k2) b ON a.ak1 = b.k1 AND a.ak1 > 0
        // The filter references only a preserved column (probe ak1), so the rule fires and the filter
        // is carried over onto the rewritten join.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(INNER, probe, build, p.rowExpression("ak1 > 0"), new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(AggregationNode.class,
                                                                        node(ValuesNode.class))))))));
    }

    @Test
    public void testDoesNotFireWhenJoinFilterReferencesPackedColumn()
    {
        // The join filter references a packed (collapsed) column (b.measure), which is unavailable at
        // the collapsed join, so the rule must decline.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(INNER, probe, build, p.rowExpression("measure > 0"), new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenJoinHasDynamicFilters()
    {
        // The rewrite replaces a side of the join (and its variables); any dynamic filter referencing
        // that side would be invalidated, so the rule must decline when dynamic filters are present.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(
                            INNER,
                            probe,
                            build,
                            ImmutableList.of(new EquiJoinClause(ak1, k1)),
                            ImmutableList.<VariableReferenceExpression>builder().addAll(probe.getOutputVariables()).add(k1).add(measure).build(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            ImmutableMap.of("df1", k1));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresCollapsingProbeSideOfInnerJoin()
    {
        // (SELECT k1, k2, sum(v) measure FROM t GROUP BY k1, k2) a JOIN b ON a.k1 = b.bk1
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    VariableReferenceExpression bk1 = p.variable("bk1", BIGINT);
                    AggregationNode probe = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    ValuesNode build = p.values(bk1);
                    return p.join(INNER, probe, build, new EquiJoinClause(k1, bk1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(AggregationNode.class,
                                                                        node(ValuesNode.class)))),
                                                node(ValuesNode.class)))));
    }

    @Test
    public void testFiresOnLeftJoinWhenAggregationOnLeft()
    {
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    VariableReferenceExpression bk1 = p.variable("bk1", BIGINT);
                    AggregationNode left = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    ValuesNode right = p.values(bk1);
                    return p.join(JoinType.LEFT, left, right, new EquiJoinClause(k1, bk1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(AggregationNode.class,
                                                                        node(ValuesNode.class)))),
                                                node(ValuesNode.class)))));
    }

    @Test
    public void testFiresOnRightJoinWhenAggregationOnRight()
    {
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode left = p.values(ak1);
                    AggregationNode right = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(JoinType.RIGHT, left, right, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(AggregationNode.class,
                                                                        node(ValuesNode.class))))))));
    }

    @Test
    public void testFiresOnLeftJoinCollapsingNullSupplyingSide()
    {
        // a LEFT JOIN (SELECT k1, k2, sum(v) measure FROM t GROUP BY k1, k2) b ON a.ak1 = b.k1
        // The fan-out is on the null-supplying (right) side. The extra ProjectNode between the join
        // and the UNNEST is the COALESCE that turns the NULL array of an unmatched probe row into a
        // single-element array holding a NULL row, so that row survives the CROSS JOIN UNNEST.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode left = p.values(ak1);
                    AggregationNode right = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(JoinType.LEFT, left, right, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(ProjectNode.class,
                                                node(JoinNode.class,
                                                        node(ValuesNode.class),
                                                        node(AggregationNode.class,
                                                                node(ProjectNode.class,
                                                                        node(AggregationNode.class,
                                                                                node(ValuesNode.class)))))))));
    }

    @Test
    public void testNullSupplyingSideWrapsThePackedArrayInCoalesceOfANullRow()
    {
        // The extra projection above an outer join is only correct if it actually rewrites the packed
        // array as COALESCE(data, ARRAY[CAST(NULL AS row(...))]); asserting the node shape alone would
        // still pass if a refactor kept the projection but changed or dropped that expression.
        PlanNode plan = tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode left = p.values(10, ak1);
                    AggregationNode right = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(10, k1, k2, v)));
                    return p.join(JoinType.LEFT, left, right, new EquiJoinClause(ak1, k1));
                })
                .get();

        UnnestNode unnest = (UnnestNode) ((ProjectNode) plan).getSource();
        ProjectNode coalesceProject = (ProjectNode) unnest.getSource();
        VariableReferenceExpression unnestedArray = unnest.getUnnestVariables().keySet().stream().collect(onlyElement());

        RowExpression assignment = coalesceProject.getAssignments().get(unnestedArray);
        assertTrue(assignment instanceof SpecialFormExpression, "expected the unnested array to be assigned an expression");
        SpecialFormExpression coalesce = (SpecialFormExpression) assignment;
        assertEquals(coalesce.getForm(), COALESCE);
        assertEquals(coalesce.getArguments().size(), 2);

        // First argument: the array the collapse aggregation produced. Second: a one-element array
        // holding a NULL row, so an unmatched row survives the UNNEST with all packed columns NULL.
        assertTrue(coalesce.getArguments().get(0) instanceof VariableReferenceExpression);
        CallExpression nullRowArray = (CallExpression) coalesce.getArguments().get(1);
        assertEquals(nullRowArray.getArguments().size(), 1);
        RowExpression nullRow = nullRowArray.getArguments().stream().collect(onlyElement());
        assertTrue(nullRow instanceof ConstantExpression, "expected a NULL row constant");
        assertEquals(((ConstantExpression) nullRow).getValue(), null);
        assertTrue(nullRow.getType() instanceof RowType, "expected the NULL constant to carry the packed row type");
        assertEquals(nullRowArray.getType(), coalesce.getArguments().get(0).getType());
    }

    @Test
    public void testFiresOnRightJoinCollapsingNullSupplyingSide()
    {
        // (SELECT k1, k2, sum(v) measure FROM t GROUP BY k1, k2) b RIGHT JOIN a ON b.k1 = a.ak1
        // Mirror image of the LEFT join case: the null-supplying side is the left one.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    VariableReferenceExpression bk1 = p.variable("bk1", BIGINT);
                    AggregationNode left = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    ValuesNode right = p.values(bk1);
                    return p.join(JoinType.RIGHT, left, right, new EquiJoinClause(k1, bk1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(ProjectNode.class,
                                                node(JoinNode.class,
                                                        node(AggregationNode.class,
                                                                node(ProjectNode.class,
                                                                        node(AggregationNode.class,
                                                                                node(ValuesNode.class)))),
                                                        node(ValuesNode.class))))));
    }

    @Test
    public void testDoesNotFireOnFullJoin()
    {
        // A side that fans out on both sides of a FULL join is almost always a modelling bug rather
        // than a shape worth optimizing, so FULL joins are left alone.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode left = p.values(ak1);
                    AggregationNode right = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(JoinType.FULL, left, right, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresOnDerivedKeyThroughLimit()
    {
        // The build side is a LIMIT over an aggregation grouped by (k1, k2). The structural walk
        // stops at the LimitNode, but the derived keys carry through it: the side is unique on
        // (k1, k2) and not on the join key k1 alone, so the directed analysis proves the fan-out.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    AggregationNode aggregation = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(10, k1, k2, v)));
                    return p.join(INNER, probe, p.limit(10, aggregation), new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(LimitNode.class,
                                                                        node(AggregationNode.class,
                                                                                node(ValuesNode.class)))))))));
    }

    @Test
    public void testDoesNotFireOnDerivedKeyEqualToJoinKey()
    {
        // Same shape, but the aggregation groups by exactly the join key: the derived key is the
        // join key itself, so the side is already unique on it and there is no fan-out to collapse.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    AggregationNode aggregation = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(10, k1, v)));
                    return p.join(INNER, probe, p.limit(10, aggregation), new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenJoinKeyEqualsGroupingKeys()
    {
        // Build side is already unique on the join key (grouped by exactly k1) — no fan-out.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, v)));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnCrossJoin()
    {
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(INNER, probe, build);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "false")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresUnderNonLegacyUnnest()
    {
        // Under non-legacy unnest the rule still fires, emitting the flattened array-of-rows form
        // (one column per field) instead of the single-ROW + dereference form. Same plan shape.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "false")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(AggregationNode.class,
                                                                        node(ValuesNode.class))))))));
    }

    @Test
    public void testFiresCollapsingInnerJoinBuildSide()
    {
        // a JOIN (b JOIN c ON b.k1=c.ck1 AND b.k2=c.ck2) ON a.ak1 = b.k1
        // The inner join's probe is grouped on (k1, k2); PropertyDerivations propagates probe local
        // properties across an INNER join, so the side reports grouping on a superset of the outer key.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    VariableReferenceExpression ck2 = p.variable("ck2", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    JoinNode build = p.join(
                            INNER,
                            p.aggregation(agg -> agg
                                    .addAggregation(bval, p.rowExpression("sum(v)"))
                                    .singleGroupingSet(k1, k2)
                                    .step(AggregationNode.Step.SINGLE)
                                    .source(p.values(10, k1, k2, v))),
                            p.values(10, ck1, ck2),
                            new EquiJoinClause(k1, ck1),
                            new EquiJoinClause(k2, ck2));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(JoinNode.class,
                                                                        node(AggregationNode.class, node(ValuesNode.class)),
                                                                        node(ValuesNode.class))))))));
    }

    @Test
    public void testFiresCollapsingInnerJoinProbeSide()
    {
        // (agg(GROUP BY k1,k2) JOIN c ON k1,k2) JOIN d ON k1 = d.dk1 — collapse on the probe side.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    VariableReferenceExpression ck2 = p.variable("ck2", BIGINT);
                    VariableReferenceExpression dk1 = p.variable("dk1", BIGINT);
                    JoinNode probe = p.join(
                            INNER,
                            p.aggregation(agg -> agg
                                    .addAggregation(bval, p.rowExpression("sum(v)"))
                                    .singleGroupingSet(k1, k2)
                                    .step(AggregationNode.Step.SINGLE)
                                    .source(p.values(10, k1, k2, v))),
                            p.values(10, ck1, ck2),
                            new EquiJoinClause(k1, ck1),
                            new EquiJoinClause(k2, ck2));
                    ValuesNode build = p.values(10, dk1);
                    return p.join(INNER, probe, build, new EquiJoinClause(k1, dk1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(JoinNode.class,
                                                                        node(AggregationNode.class, node(ValuesNode.class)),
                                                                        node(ValuesNode.class)))),
                                                node(ValuesNode.class)))));
    }

    @Test
    public void testFiresOnLeftJoinWhenInnerJoinOnLeft()
    {
        // LEFT join: the left side is an inner join whose probe is grouped on (k1, k2), so the
        // grouping reaches the outer join and the preserved side collapses.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    VariableReferenceExpression ck2 = p.variable("ck2", BIGINT);
                    VariableReferenceExpression dk1 = p.variable("dk1", BIGINT);
                    JoinNode left = p.join(
                            INNER,
                            p.aggregation(agg -> agg
                                    .addAggregation(bval, p.rowExpression("sum(v)"))
                                    .singleGroupingSet(k1, k2)
                                    .step(AggregationNode.Step.SINGLE)
                                    .source(p.values(10, k1, k2, v))),
                            p.values(10, ck1, ck2),
                            new EquiJoinClause(k1, ck1),
                            new EquiJoinClause(k2, ck2));
                    ValuesNode right = p.values(10, dk1);
                    return p.join(JoinType.LEFT, left, right, new EquiJoinClause(k1, dk1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(JoinNode.class,
                                                                        node(AggregationNode.class, node(ValuesNode.class)),
                                                                        node(ValuesNode.class)))),
                                                node(ValuesNode.class)))));
    }

    @Test
    public void testDoesNotFireWhenInnerJoinKeysEqualOuterKeys()
    {
        // The build inner join is keyed on exactly k1 (== outer key), so it is already unique on
        // the outer key — no fan-out, no extra key, must not fire.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    JoinNode build = p.join(
                            INNER,
                            p.values(k1, bval),
                            p.values(ck1),
                            new EquiJoinClause(k1, ck1));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenInnerJoinIsNotInner()
    {
        // v1 only collapses an INNER inner-join fan-out; a LEFT inner join must not fire.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    VariableReferenceExpression ck2 = p.variable("ck2", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    JoinNode build = p.join(
                            JoinType.LEFT,
                            p.values(k1, k2, bval),
                            p.values(10, ck1, ck2),
                            new EquiJoinClause(k1, ck1),
                            new EquiJoinClause(k2, ck2));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresThroughProjectAndFilterWithoutLogicalProperties()
    {
        // The common shape: a JOIN project(filter(agg GROUP BY (k1, k2))) ON k1, where the side
        // reaching the join is a projection rather than the aggregation itself. The fan-out has to
        // come from the grouping the aggregation advertises, carried up through the filter and the
        // projection.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    VariableReferenceExpression derived = p.variable("derived", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    AggregationNode aggregation = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(10, k1, k2, v)));
                    FilterNode filter = p.filter(p.rowExpression("measure > 0"), aggregation);
                    ProjectNode build = p.project(
                            Assignments.builder().put(k1, k1).put(k2, k2).put(derived, p.rowExpression("measure + 1")).build(),
                            filter);
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(ProjectNode.class,
                                                                        node(FilterNode.class,
                                                                                node(AggregationNode.class,
                                                                                        node(ValuesNode.class))))))))));
    }

    @Test
    public void testDoesNotFailOnNodeTypeWithoutDerivedProperties()
    {
        // PropertyDerivations only covers the node types the physical planner sees and throws on
        // anything else, so the rule must treat that as "no properties" rather than letting the
        // exception escape and fail the query. This matters even with the rule disabled: when
        // verbose_optimizer_info is on, IterativeOptimizer calls apply() on disabled rules too.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression rowCount = p.variable("rows", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    PlanNode build = p.cteProducerNode("cte", rowCount, ImmutableList.of(k1, k2), p.values(10, k1, k2));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresWhenAnUnsupportedNodeSitsBelowTheAggregation()
    {
        // PropertyDerivations does not cover UnionNode, but the fan-out signal comes from the
        // aggregation above it. The union reports unknown properties and derivation continues, so
        // the grouping still reaches the join.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression leftK1 = p.variable("leftK1", BIGINT);
                    VariableReferenceExpression leftK2 = p.variable("leftK2", BIGINT);
                    VariableReferenceExpression leftV = p.variable("leftV", BIGINT);
                    VariableReferenceExpression rightK1 = p.variable("rightK1", BIGINT);
                    VariableReferenceExpression rightK2 = p.variable("rightK2", BIGINT);
                    VariableReferenceExpression rightV = p.variable("rightV", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    PlanNode union = p.union(
                            ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                    .putAll(k1, leftK1, rightK1)
                                    .putAll(k2, leftK2, rightK2)
                                    .putAll(v, leftV, rightV)
                                    .build(),
                            ImmutableList.of(p.values(10, leftK1, leftK2, leftV), p.values(10, rightK1, rightK2, rightV)));
                    AggregationNode build = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(union));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(AggregationNode.class,
                                                                        node(UnionNode.class,
                                                                                node(ValuesNode.class),
                                                                                node(ValuesNode.class)))))))));
    }

    @Test
    public void testFiresThroughProjectIdentity()
    {
        // a JOIN project(identity k1, measure)(agg group by (k1, k2)) ON a.ak1 = k1
        // The rule does not look below the side; the derived key (k1, k2) survives the projection,
        // so the property framework is what reports the fan-out here.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    AggregationNode aggregation = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(10, k1, k2, v)));
                    ProjectNode build = p.project(Assignments.builder().put(k1, k1).put(k2, k2).put(measure, measure).build(), aggregation);
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(ProjectNode.class,
                                                                        node(AggregationNode.class,
                                                                                node(ValuesNode.class)))))))));
    }

    @Test
    public void testFiresThroughFilter()
    {
        // a JOIN filter(k1 > 0)(agg group by (k1, k2)) ON a.ak1 = k1
        // As above, the derived key survives the filter and reports the fan-out.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    AggregationNode aggregation = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(10, k1, k2, v)));
                    FilterNode build = p.filter(p.rowExpression("k1 > 0"), aggregation);
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(FilterNode.class,
                                                                        node(AggregationNode.class,
                                                                                node(ValuesNode.class)))))))));
    }

    @Test
    public void testDoesNotFireWhenProjectionDropsGroupingKey()
    {
        // The projection above the aggregation keeps only k1 and the measure, so the derived key
        // (k1, k2) cannot be expressed in the side's outputs and no key reaches the join. The side
        // does still fan out, but the rule decides at the join node from what the side advertises
        // and deliberately does not look below it for a fan-out source.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(10, ak1);
                    AggregationNode aggregation = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(10, k1, k2, v)));
                    ProjectNode build = p.project(assignment(k1, k1, measure, measure), aggregation);
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnComputedJoinKey()
    {
        // The outer join key is computed (k1 + 1) and the projection drops k2, so the derived key
        // (k1, k2) cannot be expressed in the side's outputs and no key reaches the join. The rule
        // does not try to reconstruct one by looking below the side.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    VariableReferenceExpression nk1 = p.variable("nk1", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode aggregation = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    ProjectNode build = p.project(assignment(nk1, p.rowExpression("k1 + 1"), measure, measure), aggregation);
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, nk1));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnCrossJoinSide()
    {
        // a JOIN (b CROSS JOIN c) ON a.ak1 = b.k1 does multiply rows, but collapsing it blocks a
        // strictly better plan: the join reordering that runs after this rule turns it into
        // (b JOIN a) CROSS JOIN c, moving the multiplication above the join with no packing at all.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression m1 = p.variable("m1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression m2 = p.variable("m2", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    JoinNode crossJoin = p.join(INNER, p.values(k1, m1), p.values(k2, m2));
                    return p.join(INNER, probe, crossJoin, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresOnSingleRowLimitDespiteNoFanout()
    {
        // Accepted imprecision: a LIMIT 1 side holds at most one row, so collapsing buys nothing, but
        // grouping properties carry no cardinality and this rule must not consult the constraints
        // framework, so the side still looks grouped on a superset of the join key. Harmless.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getMetadata()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    VariableReferenceExpression measure = p.variable("measure", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    AggregationNode aggregation = p.aggregation(agg -> agg
                            .addAggregation(measure, p.rowExpression("sum(v)"))
                            .singleGroupingSet(k1, k2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(k1, k2, v)));
                    return p.join(INNER, probe, p.limit(1, aggregation), new EquiJoinClause(ak1, k1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(ValuesNode.class),
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(LimitNode.class,
                                                                        node(AggregationNode.class,
                                                                                node(ValuesNode.class)))))))));
    }
}
