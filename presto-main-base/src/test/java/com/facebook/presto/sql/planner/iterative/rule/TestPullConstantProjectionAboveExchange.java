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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class TestPullConstantProjectionAboveExchange
        extends BaseRuleTest
{
    @Test
    public void testSingleSourceSingleConstant()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression constVar = p.variable("const_var", BIGINT);
                    return p.exchange(e -> e
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(a, a)
                                                    .put(constVar, constant(42L, BIGINT))
                                                    .build(),
                                            p.values(a)))
                            .addInputsSet(a, constVar)
                            .singleDistributionPartitioningScheme(a, constVar));
                })
                .matches(
                        project(
                                exchange(
                                        project(
                                                values(ImmutableList.of("a")))
                                                .withAlias("a", expression("a"))))
                                .withAlias("a", expression("a"))
                                .withAlias("const_var", expression("42")));
    }

    @Test
    public void testSingleSourceMultipleConstants()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression c1 = p.variable("c1", BIGINT);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    return p.exchange(e -> e
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(a, a)
                                                    .put(c1, constant(10L, BIGINT))
                                                    .put(c2, constant(20L, BIGINT))
                                                    .build(),
                                            p.values(a)))
                            .addInputsSet(a, c1, c2)
                            .singleDistributionPartitioningScheme(a, c1, c2));
                })
                .matches(
                        project(
                                exchange(
                                        project(
                                                values(ImmutableList.of("a")))
                                                .withAlias("a", expression("a"))))
                                .withAlias("a", expression("a"))
                                .withAlias("c1", expression("10"))
                                .withAlias("c2", expression("20")));
    }

    @Test
    public void testConstantInPartitioningKey()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression partKey = p.variable("part_key", BIGINT);
                    return p.exchange(e -> e
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(a, a)
                                                    .put(partKey, constant(42L, BIGINT))
                                                    .build(),
                                            p.values(a)))
                            .addInputsSet(a, partKey)
                            .fixedHashDistributionPartitioningScheme(
                                    ImmutableList.of(a, partKey),
                                    ImmutableList.of(partKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testMultiSourceSameConstants()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression c1 = p.variable("c1", BIGINT);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    VariableReferenceExpression outData = p.variable("out_data", BIGINT);
                    VariableReferenceExpression outConst = p.variable("out_const", BIGINT);
                    return p.exchange(e -> e
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(a, a)
                                                    .put(c1, constant(42L, BIGINT))
                                                    .build(),
                                            p.values(a)))
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(b, b)
                                                    .put(c2, constant(42L, BIGINT))
                                                    .build(),
                                            p.values(b)))
                            .addInputsSet(a, c1)
                            .addInputsSet(b, c2)
                            .singleDistributionPartitioningScheme(outData, outConst));
                })
                .matches(
                        project(
                                exchange(
                                        project(
                                                values(ImmutableList.of("a"))),
                                        project(
                                                values(ImmutableList.of("b")))))
                                .withAlias("out_const", expression("42")));
    }

    @Test
    public void testMultiSourceDifferentConstants()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression c1 = p.variable("c1", BIGINT);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    VariableReferenceExpression outData = p.variable("out_data", BIGINT);
                    VariableReferenceExpression outConst = p.variable("out_const", BIGINT);
                    return p.exchange(e -> e
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(a, a)
                                                    .put(c1, constant(42L, BIGINT))
                                                    .build(),
                                            p.values(a)))
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(b, b)
                                                    .put(c2, constant(99L, BIGINT))
                                                    .build(),
                                            p.values(b)))
                            .addInputsSet(a, c1)
                            .addInputsSet(b, c2)
                            .singleDistributionPartitioningScheme(outData, outConst));
                })
                .doesNotFire();
    }

    @Test
    public void testLocalExchangeDoesNotFire()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression constVar = p.variable("const_var", BIGINT);
                    return p.exchange(e -> e
                            .scope(LOCAL)
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(a, a)
                                                    .put(constVar, constant(42L, BIGINT))
                                                    .build(),
                                            p.values(a)))
                            .addInputsSet(a, constVar)
                            .singleDistributionPartitioningScheme(a, constVar));
                })
                .doesNotFire();
    }

    @Test
    public void testNoConstantsDoesNotFire()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    return p.exchange(e -> e
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(a, a)
                                                    .put(b, b)
                                                    .build(),
                                            p.values(a, b)))
                            .addInputsSet(a, b)
                            .singleDistributionPartitioningScheme(a, b));
                })
                .doesNotFire();
    }

    @Test
    public void testSessionPropertyDisabled()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression constVar = p.variable("const_var", BIGINT);
                    return p.exchange(e -> e
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(a, a)
                                                    .put(constVar, constant(42L, BIGINT))
                                                    .build(),
                                            p.values(a)))
                            .addInputsSet(a, constVar)
                            .singleDistributionPartitioningScheme(a, constVar));
                })
                .doesNotFire();
    }

    @Test
    public void testSourceNotProjectDoesNotFire()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.exchange(e -> e
                            .addSource(p.values(a))
                            .addInputsSet(a)
                            .singleDistributionPartitioningScheme(a));
                })
                .doesNotFire();
    }

    @Test
    public void testAllConstantsDoesNotFire()
    {
        tester().assertThat(new PullConstantProjectionAboveExchange())
                .setSystemProperty(PULL_CONSTANT_PROJECTION_ABOVE_EXCHANGE, "true")
                .on(p -> {
                    VariableReferenceExpression c1 = p.variable("c1", BIGINT);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.exchange(e -> e
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(c1, constant(1L, BIGINT))
                                                    .put(c2, constant(2L, BIGINT))
                                                    .build(),
                                            p.values(a)))
                            .addInputsSet(c1, c2)
                            .singleDistributionPartitioningScheme(c1, c2));
                })
                .doesNotFire();
    }
}
