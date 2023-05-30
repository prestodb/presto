package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushDownSemiJoinThroughProjectUnnest
        extends BaseRuleTest
{
    @Test
    public void testTriggerOptimization()
    {
        tester().assertThat(new PushDownSemiJoinThroughProjectUnnest(getMetadata()))
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("array", new ArrayType(BIGINT));
                    p.variable("field");
                    p.variable("k1");
                    p.variable("k2");
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.project(
                                    PlanBuilder.assignment(p.variable("source"), p.rowExpression("k1+k2")),
                                    p.unnest(
                                            p.values(p.variable("k1"), p.variable("k2"), p.variable("array", new ArrayType(BIGINT))),
                                            ImmutableList.of(p.variable("k1"), p.variable("k2")),
                                            ImmutableMap.of(p.variable("array", new ArrayType(BIGINT)), ImmutableList.of(p.variable("field"))),
                                            Optional.empty())),
                            p.values(p.variable("filter_source", BIGINT)));
                })
                .matches(
                        project(
                                project(
                                        unnest(
                                                ImmutableMap.of("array", ImmutableList.of("field")),
                                                semiJoin(
                                                        "source", "filter_source", "semi_output",
                                                        project(
                                                                ImmutableMap.of("source", expression("k1+k2")),
                                                                values("k1", "k2", "array")),
                                                        values("filter_source"))))));
    }

    @Test
    public void testNotTriggerNonDeterministic()
    {
        tester().assertThat(new PushDownSemiJoinThroughProjectUnnest(getMetadata()))
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("array", new ArrayType(BIGINT));
                    p.variable("field");
                    p.variable("k1");
                    p.variable("k2");
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.project(
                                    PlanBuilder.assignment(p.variable("source"), p.rowExpression("random()")),
                                    p.unnest(
                                            p.values(p.variable("k1"), p.variable("k2"), p.variable("array", new ArrayType(BIGINT))),
                                            ImmutableList.of(p.variable("k1"), p.variable("k2")),
                                            ImmutableMap.of(p.variable("array", new ArrayType(BIGINT)), ImmutableList.of(p.variable("field"))),
                                            Optional.empty())),
                            p.values(p.variable("filter_source", BIGINT)));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerIncludeUnnest()
    {
        tester().assertThat(new PushDownSemiJoinThroughProjectUnnest(getMetadata()))
                .on(p ->
                {
                    p.variable("source", BIGINT);
                    p.variable("filter_source", BIGINT);
                    p.variable("semi_output", BOOLEAN);
                    p.variable("array", new ArrayType(BIGINT));
                    p.variable("field");
                    p.variable("k1");
                    p.variable("k2");
                    return p.semiJoin(
                            p.variable("source"),
                            p.variable("filter_source", BIGINT),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.project(
                                    PlanBuilder.assignment(p.variable("source"), p.rowExpression("k1+field")),
                                    p.unnest(
                                            p.values(p.variable("k1"), p.variable("k2"), p.variable("array", new ArrayType(BIGINT))),
                                            ImmutableList.of(p.variable("k1"), p.variable("k2")),
                                            ImmutableMap.of(p.variable("array", new ArrayType(BIGINT)), ImmutableList.of(p.variable("field"))),
                                            Optional.empty())),
                            p.values(p.variable("filter_source", BIGINT)));
                }).doesNotFire();
    }
}
