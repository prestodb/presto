package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushDownSemiJoinThroughUnnest
        extends BaseRuleTest
{
    @Test
    public void testTriggerOptimization()
    {
        tester().assertThat(new PushDownSemiJoinThroughUnnest())
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("array", new ArrayType(BIGINT));
                    p.variable("field");
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.unnest(
                                    p.values(p.variable("source"), p.variable("array", new ArrayType(BIGINT))),
                                    ImmutableList.of(p.variable("source")),
                                    ImmutableMap.of(p.variable("array", new ArrayType(BIGINT)), ImmutableList.of(p.variable("field"))),
                                    Optional.empty()),
                            p.values(p.variable("filter_source", BIGINT)));
                })
                .matches(
                        project(
                                unnest(
                                        ImmutableMap.of("array", ImmutableList.of("field")),
                                        semiJoin(
                                                "source", "filter_source", "semi_output",
                                                values("source", "array"),
                                                values("filter_source")))));
    }

    @Test
    public void testNotTriggerSemiJoinOnUnnestVariable()
    {
        tester().assertThat(new PushDownSemiJoinThroughUnnest())
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("array", new ArrayType(BIGINT));
                    p.variable("field");
                    return p.semiJoin(
                            p.variable("field"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.unnest(
                                    p.values(p.variable("source"), p.variable("array", new ArrayType(BIGINT))),
                                    ImmutableList.of(p.variable("source")),
                                    ImmutableMap.of(p.variable("array", new ArrayType(BIGINT)), ImmutableList.of(p.variable("field"))),
                                    Optional.empty()),
                            p.values(p.variable("filter_source", BIGINT)));
                }).doesNotFire();
    }
}
