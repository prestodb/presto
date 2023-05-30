package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushDownSemiJoinThroughProjectCrossJoin
        extends BaseRuleTest
{
    @Test
    public void testTriggerOptimization()
    {
        tester().assertThat(new PushDownSemiJoinThroughProjectCrossJoin(getMetadata()))
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("left_k1");
                    p.variable("left_k2");
                    p.variable("right_k2", BIGINT);
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.project(
                                    PlanBuilder.assignment(p.variable("source"), p.rowExpression("left_k1+left_k2")),
                                    p.join(JoinNode.Type.INNER,
                                            p.values(p.variable("left_k1"), p.variable("left_k2")),
                                            p.values(p.variable("right_k1"), p.variable("right_k2")))),
                            p.values(p.variable("filter_source", BIGINT)));
                })
                .matches(
                        project(
                                project(
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(),
                                                semiJoin(
                                                        "source", "filter_source", "semi_output",
                                                        project(
                                                                ImmutableMap.of("source", expression("left_k1+left_k2")),
                                                                values("left_k1", "left_k2")),
                                                        values("filter_source")),
                                                values("right_k1", "right_k2")))));
    }

    @Test
    public void testNotTriggerNondeterministic()
    {
        tester().assertThat(new PushDownSemiJoinThroughProjectCrossJoin(getMetadata()))
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("left_k1");
                    p.variable("left_k2");
                    p.variable("right_k2", BIGINT);
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.project(
                                    PlanBuilder.assignment(p.variable("source"), p.rowExpression("random()")),
                                    p.join(JoinNode.Type.INNER,
                                            p.values(p.variable("left_k1"), p.variable("left_k2")),
                                            p.values(p.variable("right_k1"), p.variable("right_k2")))),
                            p.values(p.variable("filter_source", BIGINT)));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerFilterSide()
    {
        tester().assertThat(new PushDownSemiJoinThroughProjectCrossJoin(getMetadata()))
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("left_k1");
                    p.variable("left_k2");
                    p.variable("right_k2", BIGINT);
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.values(p.variable("source", BIGINT)),
                            p.project(
                                    PlanBuilder.assignment(p.variable("filter_source"), p.rowExpression("left_k1+left_k2")),
                                    p.join(JoinNode.Type.INNER,
                                            p.values(p.variable("left_k1"), p.variable("left_k2")),
                                            p.values(p.variable("right_k1"), p.variable("right_k2")))));
                }).doesNotFire();
    }
}
