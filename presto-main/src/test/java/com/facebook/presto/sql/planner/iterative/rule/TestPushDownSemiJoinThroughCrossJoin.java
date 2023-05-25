package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushDownSemiJoinThroughCrossJoin
        extends BaseRuleTest
{
    @Test
    public void testTriggerOptimization()
    {
        tester().assertThat(new PushDownSemiJoinThroughCrossJoin())
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("right_k2", BIGINT);
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("source"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))),
                            p.values(p.variable("filter_source", BIGINT)));
                })
                .matches(
                        project(
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(),
                                        semiJoin(
                                                "source", "filter_source", "semi_output",
                                                values("source", "left_k2"),
                                                values("filter_source")
                                        ),
                                        values("right_k1", "right_k2"))));
    }

    @Test
    public void testNotTriggerJoinOnFilterSide()
    {
        tester().assertThat(new PushDownSemiJoinThroughCrossJoin())
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("right_k2", BIGINT);
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.values(p.variable("source", BIGINT)),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("filter_source"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                }).doesNotFire();
    }
}
