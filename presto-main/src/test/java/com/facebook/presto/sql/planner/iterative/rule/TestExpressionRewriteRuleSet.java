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

import com.facebook.presto.common.type.DateType;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.apply;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;

public class TestExpressionRewriteRuleSet
        extends BaseRuleTest
{
    private ExpressionRewriteRuleSet zeroRewriter = new ExpressionRewriteRuleSet(
            (expression, context) -> new LongLiteral("0"));

    private static final FunctionCall nowCall = new FunctionCall(QualifiedName.of("now"), ImmutableList.of());
    private ExpressionRewriteRuleSet functionCallRewriter = new ExpressionRewriteRuleSet((expression, context) -> nowCall);

    private ExpressionRewriteRuleSet applyRewriter = new ExpressionRewriteRuleSet(
            (expression, context) -> new InPredicate(
                    new LongLiteral("0"),
                    new InListExpression(ImmutableList.of(
                            new LongLiteral("1"),
                            new LongLiteral("2")))));

    @Test
    public void testProjectionExpressionRewrite()
    {
        tester().assertThat(zeroRewriter.projectExpressionRewrite())
                .on(p -> p.project(
                        assignment(p.variable("y"), PlanBuilder.expression("x IS NOT NULL")),
                        p.values(p.variable("x"))))
                .matches(
                        project(ImmutableMap.of("y", expression("0")), values("x")));
    }

    @Test
    public void testProjectionExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.projectExpressionRewrite())
                .on(p -> p.project(
                        assignment(p.variable("y"), PlanBuilder.expression("0")),
                        p.values(p.variable("x"))))
                .doesNotFire();
    }

    @Test
    public void testAggregationExpressionRewrite()
    {
        tester().assertThat(new ExpressionRewriteRuleSet((expression, context) -> new SymbolReference("x")).aggregationExpressionRewrite())
                .on(p -> p.aggregation(a -> a
                        .globalGrouping()
                        .addAggregation(
                                p.variable("count_1", BIGINT),
                                new FunctionCall(QualifiedName.of("count"), ImmutableList.of(new SymbolReference("y"))),
                                ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.variable("x", BIGINT)))))
                .matches(
                        PlanMatchPattern.aggregation(
                                ImmutableMap.of("count_1", functionCall("count", ImmutableList.of("x"))),
                                values("x")));
    }

    @Test
    public void testAggregationExpressionNotRewritten()
    {
        // Aggregation expression will only rewrite argument/filter
        tester().assertThat(functionCallRewriter.aggregationExpressionRewrite())
                .on(p -> p.aggregation(a -> a
                        .globalGrouping()
                        .addAggregation(
                                p.variable("count_1", DateType.DATE),
                                nowCall,
                                ImmutableList.of())
                        .source(
                                p.values())))
                .doesNotFire();

        tester().assertThat(functionCallRewriter.aggregationExpressionRewrite())
                .on(p -> p.aggregation(a -> a
                        .globalGrouping()
                        .addAggregation(
                                p.variable("count_1", BIGINT),
                                new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                                ImmutableList.of(BIGINT))
                        .source(
                                p.values())))
                .doesNotFire();
    }

    @Test
    public void testFilterExpressionRewrite()
    {
        tester().assertThat(zeroRewriter.filterExpressionRewrite())
                .on(p -> p.filter(new LongLiteral("1"), p.values()))
                .matches(
                        filter("0", values()));
    }

    @Test
    public void testFilterExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.filterExpressionRewrite())
                .on(p -> p.filter(new LongLiteral("0"), p.values()))
                .doesNotFire();
    }

    @Test
    public void testValueExpressionRewrite()
    {
        tester().assertThat(zeroRewriter.valuesExpressionRewrite())
                .on(p -> p.values(
                        ImmutableList.of(p.variable("a")),
                        ImmutableList.of((ImmutableList.of(castToRowExpression(PlanBuilder.expression("1")))))))
                .matches(
                        values(ImmutableList.of("a"), ImmutableList.of(ImmutableList.of(new LongLiteral("0")))));
    }

    @Test
    public void testValueExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.valuesExpressionRewrite())
                .on(p -> p.values(
                        ImmutableList.of(p.variable("a")),
                        ImmutableList.of((ImmutableList.of(castToRowExpression(PlanBuilder.expression("0")))))))
                .doesNotFire();
    }

    @Test
    public void testApplyExpressionRewrite()
    {
        tester().assertThat(applyRewriter.applyExpressionRewrite())
                .on(p -> p.apply(
                        assignment(
                                p.variable("a", BIGINT),
                                new InPredicate(
                                        new LongLiteral("1"),
                                        new InListExpression(ImmutableList.of(
                                                new LongLiteral("1"),
                                                new LongLiteral("2"))))),
                        ImmutableList.of(),
                        p.values(),
                        p.values()))
                .matches(
                        apply(
                                ImmutableList.of(),
                                ImmutableMap.of("a", expression("0 IN (1, 2)")),
                                values(),
                                values()));
    }

    @Test
    public void testApplyExpressionNotRewritten()
    {
        tester().assertThat(applyRewriter.applyExpressionRewrite())
                .on(p -> p.apply(
                        assignment(
                                p.variable("a", BIGINT),
                                new InPredicate(
                                        new LongLiteral("0"),
                                        new InListExpression(ImmutableList.of(
                                                new LongLiteral("1"),
                                                new LongLiteral("2"))))),
                        ImmutableList.of(),
                        p.values(),
                        p.values()))
                .doesNotFire();
    }
}
