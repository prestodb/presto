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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DateType;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.apply;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

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
                        Assignments.of(p.symbol("y"), PlanBuilder.expression("x IS NOT NULL")),
                        p.values(p.symbol("x"))))
                .matches(
                        project(ImmutableMap.of("y", expression("0")), values("x")));
    }

    @Test
    public void testProjectionExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.projectExpressionRewrite())
                .on(p -> p.project(
                        Assignments.of(p.symbol("y"), PlanBuilder.expression("0")),
                        p.values(p.symbol("x"))))
                .doesNotFire();
    }

    @Test
    public void testAggregationExpressionRewrite()
    {
        tester().assertThat(functionCallRewriter.aggregationExpressionRewrite())
                .on(p -> p.aggregation(a -> a
                        .globalGrouping()
                        .addAggregation(
                                p.symbol("count_1", BigintType.BIGINT),
                                new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                                ImmutableList.of(BigintType.BIGINT))
                        .source(
                                p.values())))
                .matches(
                        PlanMatchPattern.aggregation(
                                ImmutableMap.of("count_1", functionCall("now", ImmutableList.of())),
                                values()));
    }

    @Test
    public void testAggregationExpressionNotRewritten()
    {
        tester().assertThat(functionCallRewriter.aggregationExpressionRewrite())
                .on(p -> p.aggregation(a -> a
                        .globalGrouping()
                        .addAggregation(
                                p.symbol("count_1", DateType.DATE),
                                nowCall,
                                ImmutableList.of())
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
                        ImmutableList.<Symbol>of(p.symbol("a")),
                        ImmutableList.of((ImmutableList.of(PlanBuilder.expression("1"))))))
                .matches(
                        values(ImmutableList.of("a"), ImmutableList.of(ImmutableList.of(new LongLiteral("0")))));
    }

    @Test
    public void testValueExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.valuesExpressionRewrite())
                .on(p -> p.values(
                        ImmutableList.<Symbol>of(p.symbol("a")),
                        ImmutableList.of((ImmutableList.of(PlanBuilder.expression("0"))))))
                .doesNotFire();
    }

    @Test
    public void testApplyExpressionRewrite()
    {
        tester().assertThat(applyRewriter.applyExpressionRewrite())
                .on(p -> p.apply(
                        Assignments.of(
                                p.symbol("a", BigintType.BIGINT),
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
                        Assignments.of(
                                p.symbol("a", BigintType.BIGINT),
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
