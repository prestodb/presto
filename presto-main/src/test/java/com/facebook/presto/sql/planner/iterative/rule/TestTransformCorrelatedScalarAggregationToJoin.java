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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestTransformCorrelatedScalarAggregationToJoin
{
    private static final QualifiedName SUM = QualifiedName.of("sum");

    private RuleTester tester;
    private FunctionRegistry functionRegistry;
    private Rule rule;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
        TypeRegistry typeRegistry = new TypeRegistry();
        functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
        rule = new TransformCorrelatedScalarAggregationToJoin(functionRegistry);
    }

    @Test
    public void doesNotFireOnPlanWithoutApplyNode()
    {
        tester.assertThat(rule)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedWithoutAggregation()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr", BIGINT)),
                        p.values(p.symbol("corr", BIGINT)),
                        p.values(p.symbol("a", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnUncorrelated()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(),
                        p.values(p.symbol("a", BIGINT)),
                        p.values(p.symbol("b", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedWithNonScalarAggregation()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr", BIGINT)),
                        p.values(p.symbol("corr", BIGINT)),
                        createSumAggregation(p, p.symbol("a", BIGINT), ImmutableList.of(ImmutableList.of(p.symbol("b", BIGINT))),
                                p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT)))))
                .doesNotFire();
    }

    @Test
    public void rewritesOnSubqueryWithoutProjection()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr", BIGINT)),
                        p.values(p.symbol("corr", BIGINT)),
                        createSumAggregation(p, p.symbol("a", BIGINT), ImmutableList.of(ImmutableList.of()),
                                p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT)))))
                .matches(
                        project(ImmutableMap.of("sum_1", expression("sum_1"), "corr", expression("corr")),
                                aggregation(ImmutableMap.of("sum_1", functionCall("sum", ImmutableList.of("a"))),
                                        join(JoinNode.Type.LEFT,
                                                ImmutableList.of(),
                                                assignUniqueId("unique",
                                                        values(ImmutableMap.of("corr", 0))),
                                                project(ImmutableMap.of("non_null", expression("true")),
                                                        values(ImmutableMap.of("a", 0, "b", 1)))))));
    }

    @Test
    public void rewritesOnSubqueryWithProjection()
    {
        tester.assertThat(rule)
                .on(p -> p.lateral(
                        ImmutableList.of(p.symbol("corr", BIGINT)),
                        p.values(p.symbol("corr", BIGINT)),
                        p.project(Assignments.of(p.symbol("expr", BIGINT), p.expression("sum + 1")),
                                createSumAggregation(p, p.symbol("a", BIGINT), ImmutableList.of(ImmutableList.of()),
                                        p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT))))))
                .matches(
                        project(ImmutableMap.of("corr", expression("corr"), "expr", expression("(\"sum_1\" + 1)")),
                                aggregation(ImmutableMap.of("sum_1", functionCall("sum", ImmutableList.of("a"))),
                                        join(JoinNode.Type.LEFT,
                                                ImmutableList.of(),
                                                assignUniqueId("unique",
                                                        values(ImmutableMap.of("corr", 0))),
                                                project(ImmutableMap.of("non_null", expression("true")),
                                                        values(ImmutableMap.of("a", 0, "b", 1)))))));
    }

    private AggregationNode createSumAggregation(PlanBuilder p, Symbol symbol, List<List<Symbol>> groupingSets, PlanNode source)
    {
        FunctionCall functionCall = new FunctionCall(SUM, ImmutableList.of(p.symbol("a", BIGINT).toSymbolReference()));
        TypeSignature typeSignature = p.getSymbols().get(symbol).getTypeSignature();
        Signature signature = functionRegistry.resolveFunction(SUM, fromTypeSignatures(ImmutableList.of(typeSignature)));

        AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(functionCall, signature, Optional.empty());

        return p.aggregation(source, ImmutableMap.of(p.symbol("sum", BIGINT), aggregation), groupingSets, AggregationNode.Step.SINGLE, Optional.empty(), Optional.empty());
    }
}
