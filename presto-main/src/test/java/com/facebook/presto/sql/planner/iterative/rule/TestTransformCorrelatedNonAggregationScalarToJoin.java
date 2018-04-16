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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class TestTransformCorrelatedNonAggregationScalarToJoin
{
    private static final TableHandle NATION = new TableHandle(new ConnectorId("local"), new TpchTableHandle("local", "nation", 1));

    private RuleTester tester;
    private FunctionRegistry functionRegistry;
    private Rule rule;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
        TypeRegistry typeRegistry = new TypeRegistry();
        functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
        rule = new TransformCorrelatedNonAggregationScalarToJoin(functionRegistry);
    }

    @Test
    public void doesNotFireOnPlanWithoutLateralNode()
    {
        tester.assertThat(rule)
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedNonScalar()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnUncorrelated()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.<Symbol>of(),
                        p.values(p.symbol("a")),
                        p.values(ImmutableList.of(p.symbol("b")), ImmutableList.of(expressions("1")))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedWithoutScan()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.filter(
                                p.expression("corr = a"),
                                p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void rewritesOnSubqueryWithoutProjection()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.filter(
                                        p.expression("corr = a"),
                                        p.tableScan(
                                                NATION,
                                                ImmutableList.of(p.symbol("a")),
                                                ImmutableMap.of(p.symbol("a"), new TpchColumnHandle("nationkey", BIGINT)))))))
                .matches(
                        project(
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("unique", "corr"),
                                                join(
                                                        JoinNode.Type.LEFT,
                                                        ImmutableList.of(),
                                                        Optional.of("corr = a"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values(ImmutableMap.of("corr", 0))),
                                                        project(
                                                                ImmutableMap.of("non_null", expression("true")),
                                                                tableScan("nation", ImmutableMap.of("a", "nationkey"))))))));
    }

    @Test
    public void rewritesOnSubqueryWithProjection()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.project(
                                        Assignments.of(p.symbol("a2"), p.expression("a * corr")),
                                        p.filter(
                                                p.expression("corr = a"),
                                                p.tableScan(
                                                        NATION,
                                                        ImmutableList.of(p.symbol("a")),
                                                        ImmutableMap.of(p.symbol("a"), new TpchColumnHandle("nationkey", BIGINT))))))))
                .matches(
                        project(
                                ImmutableMap.of("a2", expression("CASE (non_null) WHEN TRUE THEN (a * corr) ELSE null END")),
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("unique", "corr"),
                                                join(
                                                        JoinNode.Type.LEFT,
                                                        ImmutableList.of(),
                                                        Optional.of("corr = a"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values(ImmutableMap.of("corr", 0))),
                                                        project(ImmutableMap.of("non_null", expression("true")),
                                                                tableScan("nation", ImmutableMap.of("a", "nationkey"))))))));
    }

    @Test
    public void doesNotRewritesWhenCorrelationIsUsedInTwoFilters()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.project(
                                        Assignments.of(p.symbol("a2"), p.expression("a * 2")),
                                        p.filter(
                                                p.expression("corr = a"),
                                                p.filter(
                                                        p.expression("corr = a"),
                                                        p.tableScan(
                                                                NATION,
                                                                ImmutableList.of(p.symbol("a")),
                                                                ImmutableMap.of(p.symbol("a"), new TpchColumnHandle("nationkey", BIGINT)))))))))
                .doesNotFire();
    }

    @Test
    public void rewritesWithValuesAndLiteral()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.filter(
                                        p.expression("corr = 1"),
                                        p.values(p.symbol("a"))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "corr", expression("corr"),
                                        "a", expression("a")),
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("corr", "unique"),
                                                join(
                                                        JoinNode.Type.LEFT,
                                                        ImmutableList.of(),
                                                        Optional.of("corr = 1"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values(ImmutableMap.of("corr", 0))),
                                                        project(
                                                                ImmutableMap.of("non_null", expression("true")),
                                                                values(ImmutableMap.of("a", 0))))))));
    }

    private static Expression ensureScalarSubquery()
    {
        return new SimpleCaseExpression(
                new SymbolReference("is_distinct"),
                ImmutableList.of(new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
                Optional.of(new Cast(
                        new FunctionCall(
                                QualifiedName.of("fail"),
                                ImmutableList.of(
                                        new LongLiteral(Long.toString(StandardErrorCode.SUBQUERY_MULTIPLE_ROWS.ordinal())),
                                        new StringLiteral("Scalar sub-query has returned multiple rows"))),
                        StandardTypes.BOOLEAN)));
    }
}
