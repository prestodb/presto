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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.LEGACY_UNNEST;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_JOIN_FAN_OUT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestCollapseFanoutJoinWithArrayAggUnnest
        extends BaseRuleTest
{
    @Test
    public void testFiresCollapsingBuildSideOfInnerJoin()
    {
        // a JOIN (SELECT k1, k2, sum(v) measure FROM t GROUP BY k1, k2) b ON a.k1 = b.k1
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
    public void testDoesNotFireOnLeftJoinWhenAggregationOnRight()
    {
        // LEFT join preserves only the left side; the build (right) aggregation is not collapsible.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenJoinKeyEqualsGroupingKeys()
    {
        // Build side is already unique on the join key (grouped by exactly k1) — no fan-out.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
        // The build side is an INNER join keyed on (k1, k2), a strict superset of the outer key k1.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    VariableReferenceExpression ck2 = p.variable("ck2", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    JoinNode build = p.join(
                            INNER,
                            p.values(k1, k2, bval),
                            p.values(ck1, ck2),
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
                                                                        node(ValuesNode.class),
                                                                        node(ValuesNode.class))))))));
    }

    @Test
    public void testFiresCollapsingInnerJoinProbeSide()
    {
        // (b JOIN c ON b.k1=c.ck1 AND b.k2=c.ck2) a JOIN d ON a.k1 = d.dk1
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    VariableReferenceExpression ck2 = p.variable("ck2", BIGINT);
                    VariableReferenceExpression dk1 = p.variable("dk1", BIGINT);
                    JoinNode probe = p.join(
                            INNER,
                            p.values(k1, k2, bval),
                            p.values(ck1, ck2),
                            new EquiJoinClause(k1, ck1),
                            new EquiJoinClause(k2, ck2));
                    ValuesNode build = p.values(dk1);
                    return p.join(INNER, probe, build, new EquiJoinClause(k1, dk1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(JoinNode.class,
                                                                        node(ValuesNode.class),
                                                                        node(ValuesNode.class)))),
                                                node(ValuesNode.class)))));
    }

    @Test
    public void testFiresOnLeftJoinWhenInnerJoinOnLeft()
    {
        // LEFT join preserves the left side; the left inner join keyed on (k1, k2) is collapsible.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    VariableReferenceExpression ck2 = p.variable("ck2", BIGINT);
                    VariableReferenceExpression dk1 = p.variable("dk1", BIGINT);
                    JoinNode left = p.join(
                            INNER,
                            p.values(k1, k2, bval),
                            p.values(ck1, ck2),
                            new EquiJoinClause(k1, ck1),
                            new EquiJoinClause(k2, ck2));
                    ValuesNode right = p.values(dk1);
                    return p.join(JoinType.LEFT, left, right, new EquiJoinClause(k1, dk1));
                })
                .matches(
                        node(ProjectNode.class,
                                node(UnnestNode.class,
                                        node(JoinNode.class,
                                                node(AggregationNode.class,
                                                        node(ProjectNode.class,
                                                                node(JoinNode.class,
                                                                        node(ValuesNode.class),
                                                                        node(ValuesNode.class)))),
                                                node(ValuesNode.class)))));
    }

    @Test
    public void testDoesNotFireWhenInnerJoinKeysEqualOuterKeys()
    {
        // The build inner join is keyed on exactly k1 (== outer key), so it is already unique on
        // the outer key — no fan-out, no extra key, must not fire.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
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
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_JOIN_FAN_OUT, "true")
                .setSystemProperty(LEGACY_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression ak1 = p.variable("ak1", BIGINT);
                    VariableReferenceExpression k1 = p.variable("k1", BIGINT);
                    VariableReferenceExpression k2 = p.variable("k2", BIGINT);
                    VariableReferenceExpression bval = p.variable("bval", BIGINT);
                    VariableReferenceExpression ck1 = p.variable("ck1", BIGINT);
                    VariableReferenceExpression ck2 = p.variable("ck2", BIGINT);
                    ValuesNode probe = p.values(ak1);
                    JoinNode build = p.join(
                            JoinType.LEFT,
                            p.values(k1, k2, bval),
                            p.values(ck1, ck2),
                            new EquiJoinClause(k1, ck1),
                            new EquiJoinClause(k2, ck2));
                    return p.join(INNER, probe, build, new EquiJoinClause(ak1, k1));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresPeeringThroughProjectIdentity()
    {
        // a JOIN project(identity k1, measure)(agg group by (k1, k2)) ON a.ak1 = k1
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
                    ProjectNode build = p.project(assignment(k1, k1, measure, measure), aggregation);
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
    public void testFiresPeeringThroughFilter()
    {
        // a JOIN filter(k1 > 0)(agg group by (k1, k2)) ON a.ak1 = k1
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
    public void testDoesNotFirePeeringThroughNonIdentityProject()
    {
        // The outer join key is produced by a non-identity expression (k1 + 1) over the fan-out
        // source, so the tracked key cannot be traced down — must not fire.
        tester().assertThat(new CollapseFanoutJoinWithArrayAggUnnest(getFunctionManager()))
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
}
