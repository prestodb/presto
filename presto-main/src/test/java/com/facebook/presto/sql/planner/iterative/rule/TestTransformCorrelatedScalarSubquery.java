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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.lateral;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class TestTransformCorrelatedScalarSubquery
        extends BaseRuleTest
{
    private static final ImmutableList<List<RowExpression>> ONE_ROW = ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)));
    private static final ImmutableList<List<RowExpression>> TWO_ROWS = ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)), ImmutableList.of(constant(2L, BIGINT)));

    private Rule rule = new TransformCorrelatedScalarSubquery();

    @Test
    public void doesNotFireOnPlanWithoutLateralNode()
    {
        tester().assertThat(rule)
                .on(p -> p.values(p.variable("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedNonScalar()
    {
        tester().assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.variable("corr")),
                        p.values(p.variable("corr")),
                        p.values(p.variable("a"))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnUncorrelated()
    {
        tester().assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(),
                        p.values(p.variable("a")),
                        p.values(ImmutableList.of(p.variable("b")), ImmutableList.of(constantExpressions(BIGINT, 1L)))))
                .doesNotFire();
    }

    @Test
    public void rewritesOnSubqueryWithoutProjection()
    {
        tester().assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.variable("corr")),
                        p.values(p.variable("corr")),
                        p.enforceSingleRow(
                                p.filter(
                                        p.expression("1 = a"), // TODO use correlated predicate, it requires support for correlated subqueries in plan matchers
                                        p.values(ImmutableList.of(p.variable("a")), TWO_ROWS)))))
                .matches(
                        project(
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("corr", "unique"),
                                                lateral(
                                                        ImmutableList.of("corr"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")),
                                                        filter(
                                                                "1 = a",
                                                                values("a")))))));
    }

    @Test
    public void rewritesOnSubqueryWithProjection()
    {
        tester().assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.variable("corr")),
                        p.values(p.variable("corr")),
                        p.enforceSingleRow(
                                p.project(
                                        assignment(p.variable("a2"), p.expression("a * 2")),
                                        p.filter(
                                                p.expression("1 = a"), // TODO use correlated predicate, it requires support for correlated subqueries in plan matchers
                                                p.values(ImmutableList.of(p.variable("a")), TWO_ROWS))))))
                .matches(
                        project(
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("corr", "unique"),
                                                lateral(
                                                        ImmutableList.of("corr"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")),
                                                        project(ImmutableMap.of("a2", expression("a * 2")),
                                                                filter("1 = a",
                                                                        values("a"))))))));
    }

    @Test
    public void rewritesOnSubqueryWithProjectionOnTopEnforceSingleNode()
    {
        tester().assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.variable("corr")),
                        p.values(p.variable("corr")),
                        p.project(
                                assignment(p.variable("a3"), p.expression("a2 + 1")),
                                p.enforceSingleRow(
                                        p.project(
                                                assignment(p.variable("a2"), p.expression("a * 2")),
                                                p.filter(
                                                        p.expression("1 = a"), // TODO use correlated predicate, it requires support for correlated subqueries in plan matchers
                                                        p.values(ImmutableList.of(p.variable("a")), TWO_ROWS)))))))
                .matches(
                        project(
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("corr", "unique"),
                                                lateral(
                                                        ImmutableList.of("corr"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")),
                                                        project(
                                                                ImmutableMap.of("a3", expression("a2 + 1")),
                                                                project(
                                                                        ImmutableMap.of("a2", expression("a * 2")),
                                                                        filter(
                                                                                "1 = a",
                                                                                values("a")))))))));
    }

    @Test
    public void rewritesScalarSubquery()
    {
        tester().assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.variable("corr")),
                        p.values(p.variable("corr")),
                        p.enforceSingleRow(
                                p.filter(
                                        p.expression("1 = a"), // TODO use correlated predicate, it requires support for correlated subqueries in plan matchers
                                        p.values(ImmutableList.of(p.variable("a")), ONE_ROW)))))
                .matches(
                        lateral(
                                ImmutableList.of("corr"),
                                values("corr"),
                                filter(
                                        "1 = a",
                                        values("a"))));
    }

    private static Expression ensureScalarSubquery()
    {
        return new SimpleCaseExpression(
                new SymbolReference("is_distinct"),
                ImmutableList.of(new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
                Optional.of(new Cast(new FunctionCall(
                        QualifiedName.of("fail"),
                        ImmutableList.of(
                                new LongLiteral(Long.toString(StandardErrorCode.SUBQUERY_MULTIPLE_ROWS.ordinal())),
                                new StringLiteral("Scalar sub-query has returned multiple rows"))),
                        StandardTypes.BOOLEAN)));
    }
}
