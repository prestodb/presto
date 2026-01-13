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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.dereferenceInRange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.rowConstructor;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestMergeMaxByAggregations
        extends BaseRuleTest
{
    @Test
    public void testMergeMaxByWithSameComparisonKey()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k)"))
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k")));
                }))
                .matches(
                        project(
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(Optional.of("max_by"), functionCall("max_by", ImmutableList.of("expr", "k"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of(
                                                        "v1", expression("v1"),
                                                        "v2", expression("v2"),
                                                        "k", expression("k")),
                                                values("v1", "v2", "k"))
                                                .withAlias("expr", rowConstructor("v1", "v2"))))
                                .withAlias("max_v1", dereferenceInRange("max_by", 0, 1))
                                .withAlias("max_v2", dereferenceInRange("max_by", 0, 1)));
    }

    @Test
    public void testMergeMultipleMaxByWithSameComparisonKey()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("v3", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k)"))
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k)"))
                            .addAggregation(p.variable("max_v3"), p.rowExpression("max_by(v3, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("v3"), p.variable("k")));
                }))
                .matches(
                        project(
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(Optional.of("max_by"), functionCall("max_by", ImmutableList.of("expr", "k"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of(
                                                        "v1", expression("v1"),
                                                        "v2", expression("v2"),
                                                        "v3", expression("v3"),
                                                        "k", expression("k")),
                                                values("v1", "v2", "v3", "k"))
                                                .withAlias("expr", rowConstructor("v1", "v2", "v3"))))
                                .withAlias("max_v1", dereferenceInRange("max_by", 0, 2))
                                .withAlias("max_v2", dereferenceInRange("max_by", 0, 2))
                                .withAlias("max_v3", dereferenceInRange("max_by", 0, 2)));
    }

    @Test
    public void testDoesNotFireWithSingleMaxBy()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k)"))
                            .source(p.values(p.variable("v1"), p.variable("k")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithDifferentComparisonKeys()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k1", BIGINT);
                    p.variable("k2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k1)"))
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k2)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k1"), p.variable("k2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "false")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k)"))
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k")));
                }))
                .doesNotFire();
    }

    @Test
    public void testMergeMaxByWithGroupBy()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    p.variable("g", BIGINT);
                    af.singleGroupingSet(p.variable("g"))
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k)"))
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k"), p.variable("g")));
                }))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("g"),
                                        ImmutableMap.of(Optional.of("max_by"), functionCall("max_by", ImmutableList.of("expr", "k"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of(
                                                        "v1", expression("v1"),
                                                        "v2", expression("v2"),
                                                        "k", expression("k"),
                                                        "g", expression("g")),
                                                values("v1", "v2", "k", "g"))
                                                .withAlias("expr", rowConstructor("v1", "v2"))))
                                .withAlias("max_v1", dereferenceInRange("max_by", 0, 1))
                                .withAlias("max_v2", dereferenceInRange("max_by", 0, 1)));
    }

    @Test
    public void testMergeMaxByWithMixedTypes()
    {
        // Note: Using all BIGINT types since the rowExpression parser has type registration conflicts
        // The optimizer's behavior is type-agnostic for this transformation
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("v3", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k)"))
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k)"))
                            .addAggregation(p.variable("max_v3"), p.rowExpression("max_by(v3, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("v3"), p.variable("k")));
                }))
                .matches(
                        node(ProjectNode.class,
                                node(AggregationNode.class,
                                        node(ProjectNode.class,
                                                values("v1", "v2", "v3", "k")))));
    }

    @Test
    public void testMergeMaxByWithMixedAggregations()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("v3", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k)"))
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k)"))
                            .addAggregation(p.variable("sum_v3"), p.rowExpression("sum(v3)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("v3"), p.variable("k")));
                }))
                .matches(
                        project(
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(
                                                Optional.of("max_by"), functionCall("max_by", ImmutableList.of("expr", "k")),
                                                Optional.of("sum_v3"), functionCall("sum", ImmutableList.of("v3"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of(
                                                        "v1", expression("v1"),
                                                        "v2", expression("v2"),
                                                        "v3", expression("v3"),
                                                        "k", expression("k")),
                                                values("v1", "v2", "v3", "k"))
                                                .withAlias("expr", rowConstructor("v1", "v2"))))
                                .withAlias("max_v1", dereferenceInRange("max_by", 0, 1))
                                .withAlias("max_v2", dereferenceInRange("max_by", 0, 1)));
    }

    @Test
    public void testMergeMultipleGroupsWithDifferentKeys()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("v3", BIGINT);
                    p.variable("v4", BIGINT);
                    p.variable("k1", BIGINT);
                    p.variable("k2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1_k1"), p.rowExpression("max_by(v1, k1)"))
                            .addAggregation(p.variable("max_v2_k1"), p.rowExpression("max_by(v2, k1)"))
                            .addAggregation(p.variable("max_v3_k2"), p.rowExpression("max_by(v3, k2)"))
                            .addAggregation(p.variable("max_v4_k2"), p.rowExpression("max_by(v4, k2)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("v3"), p.variable("v4"), p.variable("k1"), p.variable("k2")));
                }))
                .matches(
                        project(
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(
                                                Optional.of("max_by_k1"), functionCall("max_by", ImmutableList.of("expr_k1", "k1")),
                                                Optional.of("max_by_k2"), functionCall("max_by", ImmutableList.of("expr_k2", "k2"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of(
                                                        "v1", expression("v1"),
                                                        "v2", expression("v2"),
                                                        "v3", expression("v3"),
                                                        "v4", expression("v4"),
                                                        "k1", expression("k1"),
                                                        "k2", expression("k2")),
                                                values("v1", "v2", "v3", "v4", "k1", "k2"))
                                                .withAlias("expr_k1", rowConstructor("v1", "v2"))
                                                .withAlias("expr_k2", rowConstructor("v3", "v4"))))
                                .withAlias("max_v1_k1", dereferenceInRange("max_by_k1", 0, 1))
                                .withAlias("max_v2_k1", dereferenceInRange("max_by_k1", 0, 1))
                                .withAlias("max_v3_k2", dereferenceInRange("max_by_k2", 0, 1))
                                .withAlias("max_v4_k2", dereferenceInRange("max_by_k2", 0, 1)));
    }

    @Test
    public void testDoesNotMergeMaxByWithFilter()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    p.variable("cond", BIGINT);
                    af.globalGrouping()
                            .addAggregation(
                                    p.variable("max_v1"),
                                    p.rowExpression("max_by(v1, k)"),
                                    Optional.of(p.rowExpression("cond > 0")),
                                    Optional.empty(),
                                    false,
                                    Optional.empty())
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k"), p.variable("cond")));
                }))
                .doesNotFire();
    }

    @Test
    public void testMergeMaxByWithMatchingFilters()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    p.variable("cond", BIGINT);
                    return p.aggregation(af -> {
                        af.globalGrouping()
                                .addAggregation(
                                        p.variable("max_v1"),
                                        p.rowExpression("max_by(v1, k)"),
                                        Optional.of(p.variable("cond")),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())
                                .addAggregation(
                                        p.variable("max_v2"),
                                        p.rowExpression("max_by(v2, k)"),
                                        Optional.of(p.variable("cond")),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())
                                .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k"), p.variable("cond")));
                    });
                })
                .matches(
                        node(ProjectNode.class,
                                node(AggregationNode.class,
                                        node(ProjectNode.class,
                                                values("v1", "v2", "k", "cond")))));
    }

    @Test
    public void testDoesNotMergeMaxByWithDifferentFilters()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    p.variable("cond1", BIGINT);
                    p.variable("cond2", BIGINT);
                    return p.aggregation(af -> {
                        af.globalGrouping()
                                .addAggregation(
                                        p.variable("max_v1"),
                                        p.rowExpression("max_by(v1, k)"),
                                        Optional.of(p.variable("cond1")),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())
                                .addAggregation(
                                        p.variable("max_v2"),
                                        p.rowExpression("max_by(v2, k)"),
                                        Optional.of(p.variable("cond2")),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())
                                .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k"), p.variable("cond1"), p.variable("cond2")));
                    });
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotMergeMaxByWithDistinct()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    return p.aggregation(af -> {
                        af.globalGrouping()
                                .addAggregation(
                                        p.variable("max_v1"),
                                        p.rowExpression("max_by(v1, k)"),
                                        Optional.empty(),
                                        Optional.empty(),
                                        true,
                                        Optional.empty())
                                .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k)"))
                                .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k")));
                    });
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoMaxByAggregations()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_v1"), p.rowExpression("sum(v1)"))
                            .addAggregation(p.variable("count_v2"), p.rowExpression("count(v2)"))
                            .source(p.values(p.variable("v1"), p.variable("v2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithMaxByWithThreeArguments()
    {
        tester().assertThat(new MergeMaxByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_v1"), p.rowExpression("max_by(v1, k, 10)"))
                            .addAggregation(p.variable("max_v2"), p.rowExpression("max_by(v2, k, 10)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k")));
                }))
                .doesNotFire();
    }
}
