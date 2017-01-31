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
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.apply;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestTransformExistsApplyToScalarApply
{
    private final RuleTester tester = new RuleTester();
    private final TypeRegistry typeManager = new TypeRegistry();
    private final FunctionRegistry registry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
    private final Rule transformExistsApplyToScalarApply = new TransformExistsApplyToScalarApply(registry);

    @Test
    public void testDoesNotFire()
    {
        tester.assertThat(transformExistsApplyToScalarApply)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();

        tester.assertThat(transformExistsApplyToScalarApply)
                .on(p ->
                        p.apply(
                                Assignments.identity(p.symbol("a", BIGINT), p.symbol("b", BIGINT)),
                                ImmutableList.of(p.symbol("a", BIGINT)),
                                p.values(p.symbol("a", BIGINT)),
                                p.values(p.symbol("a", BIGINT)))
                )
                .doesNotFire();

        tester.assertThat(transformExistsApplyToScalarApply)
                .on(p ->
                        p.apply(
                                Assignments.identity(p.symbol("a", BIGINT)),
                                ImmutableList.of(p.symbol("a", BIGINT)),
                                p.values(p.symbol("a", BIGINT)),
                                p.values(p.symbol("a", BIGINT)))
                )
                .doesNotFire();

        tester.assertThat(transformExistsApplyToScalarApply)
                .on(p ->
                        p.apply(
                                Assignments.of(p.symbol("b", BOOLEAN), expression("\"a\"")),
                                ImmutableList.of(),
                                p.values(),
                                p.values(p.symbol("a", BIGINT)))
                )
                .doesNotFire();
    }

    @Test
    public void testRewrite()
            throws Exception
    {
        tester.assertThat(transformExistsApplyToScalarApply)
                .on(p ->
                        p.apply(
                                Assignments.of(p.symbol("b", BOOLEAN), expression("EXISTS(SELECT \"a\")")),
                                ImmutableList.of(),
                                p.values(),
                                p.values(p.symbol("a", BIGINT)))
                )
                .matches(apply(
                        ImmutableList.of(),
                        ImmutableMap.of("b", PlanMatchPattern.expression("\"b\"")),
                        values(ImmutableMap.of()),
                        project(
                                ImmutableMap.of("b", PlanMatchPattern.expression("(\"count_expr\" > CAST(0 AS bigint))")),
                                aggregation(ImmutableMap.of("count_expr", functionCall("count", ImmutableList.of())),
                                        limit(1, values(ImmutableMap.of("a", 0)))))));
    }
}
