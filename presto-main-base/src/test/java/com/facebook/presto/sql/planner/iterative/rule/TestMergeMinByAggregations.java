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

public class TestMergeMinByAggregations
        extends BaseRuleTest
{
    @Test
    public void testMergeMinByWithSameComparisionKey()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k)"))
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k")));
                }))
                .matches(
                        project(
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(Optional.of("min_by"), functionCall("min_by", ImmutableList.of("expr", "k"))),
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
                                .withAlias("min_v1", dereferenceInRange("min_by", 0, 1))
                                .withAlias("min_v2", dereferenceInRange("min_by", 0, 1)));
    }

    @Test
    public void testMergeMultipleMinByWithSameComparisionKey()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("v3", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k)"))
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k)"))
                            .addAggregation(p.variable("min_v3"), p.rowExpression("min_by(v3, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("v3"), p.variable("k")));
                }))
                .matches(
                        project(
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(Optional.of("min_by"), functionCall("min_by", ImmutableList.of("expr", "k"))),
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
                                .withAlias("min_v1", dereferenceInRange("min_by", 0, 2))
                                .withAlias("min_v2", dereferenceInRange("min_by", 0, 2))
                                .withAlias("min_v3", dereferenceInRange("min_by", 0, 2)));
    }

    @Test
    public void testDoesNotFireWithSingleMinBy()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k)"))
                            .source(p.values(p.variable("v1"), p.variable("k")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithDifferentComparisonKeys()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k1", BIGINT);
                    p.variable("k2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k1)"))
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k2)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k1"), p.variable("k2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "false")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k)"))
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k")));
                }))
                .doesNotFire();
    }

    @Test
    public void testMergedMinByWithGroupBy()
    {
        tester.assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation((af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    p.variable("g", BIGINT);
                    af.singleGroupingSet(p.variable("g"))
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k)"))
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k"), p.variable("g")));
                })))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("g"),
                                        ImmutableMap.of(Optional.of("min_by"), functionCall("min_by", ImmutableList.of("expr", "k"))),
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
                                .withAlias("min_v1", dereferenceInRange("min_by", 0, 1))
                                .withAlias("min_v2", dereferenceInRange("min_by", 0, 1)));
    }

    @Test
    public void testMergeMinByWithMixedType()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("v3", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k)"))
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k)"))
                            .addAggregation(p.variable("min_v3"), p.rowExpression("min_by(v3, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("v3"), p.variable("k")));
                }))
                .matches(
                        node(ProjectNode.class,
                                node(AggregationNode.class,
                                        node(ProjectNode.class,
                                                values("v1", "v2", "v3", "k")))));
    }

    @Test
    public void testMergeMinByWithMixedAggregations()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("v3", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k)"))
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k)"))
                            .addAggregation(p.variable("sum_v3"), p.rowExpression("sum(v3)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("v3"), p.variable("k")));
                }))
                .matches(
                        project(
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(
                                                Optional.of("min_by"), functionCall("min_by", ImmutableList.of("expr", "k")),
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
                                .withAlias("min_v1", dereferenceInRange("min_by", 0, 1))
                                .withAlias("min_v2", dereferenceInRange("min_by", 0, 1)));
    }

    @Test
    public void testMergeMultipleGroupsWithDifferentKeys()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("v3", BIGINT);
                    p.variable("v4", BIGINT);
                    p.variable("k1", BIGINT);
                    p.variable("k2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1_k1"), p.rowExpression("min_by(v1, k1)"))
                            .addAggregation(p.variable("min_v2_k1"), p.rowExpression("min_by(v2, k1)"))
                            .addAggregation(p.variable("min_v3_k2"), p.rowExpression("min_by(v3, k2)"))
                            .addAggregation(p.variable("min_v4_k2"), p.rowExpression("min_by(v4, k2)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("v3"), p.variable("v4"), p.variable("k1"), p.variable("k2")));
                }))
                .matches(
                        project(
                                aggregation(
                                        globalAggregation(),
                                        ImmutableMap.of(
                                                Optional.of("min_by_k1"), functionCall("min_by", ImmutableList.of("expr_k1", "k1")),
                                                Optional.of("min_by_k2"), functionCall("min_by", ImmutableList.of("expr_k2", "k2"))),
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
                                .withAlias("min_v1_k1", dereferenceInRange("min_by_k1", 0, 1))
                                .withAlias("min_v2_k1", dereferenceInRange("min_by_k1", 0, 1))
                                .withAlias("min_v3_k2", dereferenceInRange("min_by_k2", 0, 1))
                                .withAlias("min_v4_k2", dereferenceInRange("min_by_k2", 0, 1)));
    }

    @Test
    public void testDoesNotMergeMinByWithFilter()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    p.variable("cond", BIGINT);
                    af.globalGrouping()
                            .addAggregation(
                                    p.variable("min_v1"),
                                    p.rowExpression("min_by(v1, k)"),
                                    Optional.of(p.rowExpression("cond > 0")),
                                    Optional.empty(),
                                    false,
                                    Optional.empty())
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k"), p.variable("cond")));
                }))
                .doesNotFire();
    }

    @Test
    public void testMergeMinByWithMatchingFilters()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    p.variable("cond", BIGINT);
                    return p.aggregation((af -> {
                        af.globalGrouping()
                                .addAggregation(
                                        p.variable("min_v1"),
                                        p.rowExpression("min_by(v1, k)"),
                                        Optional.of(p.variable("cond")),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())
                                .addAggregation(
                                        p.variable("min_v2"),
                                        p.rowExpression("min_by(v2, k)"),
                                        Optional.of(p.variable("cond")),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())
                                .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k"), p.variable("cond")));
                    }));
                })
                .matches(
                        node(ProjectNode.class,
                                node(AggregationNode.class,
                                        node(ProjectNode.class,
                                                values("v1", "v2", "k", "cond")))));
    }

    @Test
    public void testDoesNotMergeMinByWithDifferentFilters()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
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
                                        p.variable("min_v1"),
                                        p.rowExpression("min_by(v1, k)"),
                                        Optional.of(p.variable("cond1")),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())
                                .addAggregation(
                                        p.variable("min_v2"),
                                        p.rowExpression("min_by(v2, k)"),
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
    public void testDoesNotMergeMinByWithDistinct()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    return p.aggregation(af -> {
                        af.globalGrouping()
                                .addAggregation(
                                        p.variable("min_v1"),
                                        p.rowExpression("max_by(v1, k)"),
                                        Optional.empty(),
                                        Optional.empty(),
                                        true,
                                        Optional.empty())
                                .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k)"))
                                .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k")));
                    });
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoMinByAggregations()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
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
    public void testDoesNotFireWithMinByWithThreeArguments()
    {
        tester().assertThat(new MergeMinByAggregations(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_MAX_BY_AND_MIN_BY_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("v1", BIGINT);
                    p.variable("v2", BIGINT);
                    p.variable("k", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("min_v1"), p.rowExpression("min_by(v1, k, 10)"))
                            .addAggregation(p.variable("min_v2"), p.rowExpression("min_by(v2, k, 10)"))
                            .source(p.values(p.variable("v1"), p.variable("v2"), p.variable("k")));
                }))
                .doesNotFire();
    }
}
