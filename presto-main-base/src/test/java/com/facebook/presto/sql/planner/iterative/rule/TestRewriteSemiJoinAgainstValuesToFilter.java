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

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.REWRITE_SEMI_JOIN_AGAINST_VALUES_TO_FILTER_MAX_SIZE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.coalesceNullToFalse;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.not;

public class TestRewriteSemiJoinAgainstValuesToFilter
        extends BaseRuleTest
{
    private FunctionAndTypeManager functionAndTypeManager()
    {
        return getMetadata().getFunctionAndTypeManager();
    }

    private RowExpression negativeAnchor(VariableReferenceExpression semiJoinOutput)
    {
        return not(functionAndTypeManager(), coalesceNullToFalse(semiJoinOutput));
    }

    private RowExpression positiveAnchor(VariableReferenceExpression semiJoinOutput)
    {
        return coalesceNullToFalse(semiJoinOutput);
    }

    private RowExpression bareNegativeAnchor(VariableReferenceExpression semiJoinOutput)
    {
        return not(functionAndTypeManager(), semiJoinOutput);
    }

    @Test
    public void testNegativeForm()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constant(2L, BIGINT)),
                                            ImmutableList.of(constant(3L, BIGINT))))));
                })
                .matches(project(filter("NOT (k IN (1, 2, 3)) OR k IS NULL", values("k"))));
    }

    @Test
    public void testPositiveForm()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            positiveAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constant(2L, BIGINT)),
                                            ImmutableList.of(constant(3L, BIGINT))))));
                })
                .matches(project(filter("k IN (1, 2, 3)", values("k"))));
    }

    @Test
    public void testNullInValuesStripped()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constant(2L, BIGINT)),
                                            ImmutableList.of(constantNull(BIGINT))))));
                })
                .matches(project(filter("NOT (k IN (1, 2)) OR k IS NULL", values("k"))));
    }

    @Test
    public void testBarePositiveForm()
    {
        // `x IN (SELECT ...)` produces a bare semiJoinOutput anchor (no coalesce wrapper).
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            sjo,
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constant(2L, BIGINT)),
                                            ImmutableList.of(constant(3L, BIGINT))))));
                })
                .matches(project(filter("k IN (1, 2, 3)", values("k"))));
    }

    @Test
    public void testBareNegativeForm()
    {
        // `x NOT IN (SELECT ...)` is SQL NOT IN: null source rows are dropped, so NO `OR k IS NULL`.
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            bareNegativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constant(2L, BIGINT)),
                                            ImmutableList.of(constant(3L, BIGINT))))));
                })
                .matches(project(filter("NOT (k IN (1, 2, 3))", values("k"))));
    }

    @Test
    public void testBareNegativeWithNullInValuesIsFalse()
    {
        // SQL `NOT IN (..., NULL)` keeps no rows -> the collapsed predicate is constant FALSE.
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            bareNegativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constantNull(BIGINT))))));
                })
                .matches(project(filter("false", values("k"))));
    }

    @Test
    public void testMatchesThroughDistinctAggregation()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.aggregation(b -> b.singleGroupingSet(fk)
                                            .source(p.values(ImmutableList.of(fk), ImmutableList.of(
                                                    ImmutableList.of(constant(1L, BIGINT)),
                                                    ImmutableList.of(constant(2L, BIGINT))))))));
                })
                .matches(project(filter("NOT (k IN (1, 2)) OR k IS NULL", values("k"))));
    }

    @Test
    public void testKeepsRemainingConjuncts()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            and(p.rowExpression("k > 10"), negativeAnchor(sjo)),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT))))));
                })
                .matches(project(filter("k > 10 AND (NOT (k IN (1)) OR k IS NULL)", values("k"))));
    }

    @Test
    public void testDoesNotMatchNonValuesFilteringSource()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.filter(p.rowExpression("true"), p.values(fk))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotMatchWithoutAnchor()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            p.rowExpression("k > 10"),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotMatchWhenOutputUsedElsewhere()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            and(negativeAnchor(sjo), sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT))))));
                })
                .doesNotFire();
    }

    @Test
    public void testIdempotent()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    return p.filter(p.rowExpression("NOT (k IN (1, 2, 3)) OR k IS NULL"), p.values(k));
                })
                .doesNotFire();
    }

    @Test
    public void testReprojectsSemiJoinOutputAsEquivalentIn()
    {
        // semiJoinOutput is preserved (for schema) as the equivalent IN over the full literal list.
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            sjo,
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constant(2L, BIGINT)),
                                            ImmutableList.of(constant(3L, BIGINT))))));
                })
                .matches(strictProject(
                        ImmutableMap.of("k", expression("k"), "sjo", expression("k IN (1, 2, 3)")),
                        filter("k IN (1, 2, 3)", values("k"))));
    }

    @Test
    public void testDoesNotMatchThroughNonDistinctAggregation()
    {
        // An aggregation with an aggregate function is not a pure DISTINCT; it must not be seen through.
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.aggregation(b -> b.singleGroupingSet(fk)
                                            .addAggregation(p.variable("cnt", BIGINT), p.rowExpression("count()"))
                                            .source(p.values(ImmutableList.of(fk), ImmutableList.of(
                                                    ImmutableList.of(constant(1L, BIGINT))))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotMatchMultiColumnValues()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression fk2 = p.variable("fk2", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk, fk2), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT), constant(2L, BIGINT))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotMatchNonConstantValuesCell()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(p.rowExpression("BIGINT '2' * BIGINT '3'"))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotMatchEqualsTrueWrapper()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            p.rowExpression("sjo = true"),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotMatchTwoAnchors()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            and(positiveAnchor(sjo), bareNegativeAnchor(sjo)),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT))))));
                })
                .doesNotFire();
    }

    @Test
    public void testEmptyValuesPositiveIsFalse()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            positiveAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of())));
                })
                .matches(project(filter("false", values("k"))));
    }

    @Test
    public void testAllNullValuesNegativeCoalesceIsTrue()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constantNull(BIGINT))))));
                })
                .matches(project(filter("true", values("k"))));
    }

    @Test
    public void testAllNullValuesBareNegativeIsFalse()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            bareNegativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constantNull(BIGINT))))));
                })
                .matches(project(filter("false", values("k"))));
    }

    @Test
    public void testEmptyValuesBareNegativeIsTrue()
    {
        // SQL NOT IN against an empty subquery keeps every row (including null source rows).
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            bareNegativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of())));
                })
                .matches(project(filter("true", values("k"))));
    }

    @Test
    public void testDisabledByZeroMaxSize()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .setSystemProperty(REWRITE_SEMI_JOIN_AGAINST_VALUES_TO_FILTER_MAX_SIZE, "0")
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT))))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireAboveMaxSize()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .setSystemProperty(REWRITE_SEMI_JOIN_AGAINST_VALUES_TO_FILTER_MAX_SIZE, "2")
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constant(2L, BIGINT)),
                                            ImmutableList.of(constant(3L, BIGINT))))));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresAtMaxSizeBoundary()
    {
        tester().assertThat(new RewriteSemiJoinAgainstValuesToFilter(functionAndTypeManager()))
                .setSystemProperty(REWRITE_SEMI_JOIN_AGAINST_VALUES_TO_FILTER_MAX_SIZE, "3")
                .on(p ->
                {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression fk = p.variable("fk", BIGINT);
                    VariableReferenceExpression sjo = p.variable("sjo", BOOLEAN);
                    return p.filter(
                            negativeAnchor(sjo),
                            p.semiJoin(k, fk, sjo, Optional.empty(), Optional.empty(),
                                    p.values(k),
                                    p.values(ImmutableList.of(fk), ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constant(2L, BIGINT)),
                                            ImmutableList.of(constant(3L, BIGINT))))));
                })
                .matches(project(filter("NOT (k IN (1, 2, 3)) OR k IS NULL", values("k"))));
    }
}
