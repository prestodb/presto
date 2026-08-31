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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.PULL_ROW_LOCAL_CHAIN_ABOVE_EXCHANGE_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestPullRowLocalChainAboveExchange
        extends BaseRuleTest
{
    private static final ArrayType ARRAY_OF_BIGINT = new ArrayType(BIGINT);

    // Exchange[REPARTITION on partitionKey] -> Project -> Unnest(arr -> elem) -> Values(k, arr).
    // When partitionOnUnnestedVariable is false the key is the replicate column k (legal to pull up);
    // when true it is the unnested output elem (illegal).
    private static Function<PlanBuilder, PlanNode> repartitionOverUnnest(boolean partitionOnUnnestedVariable)
    {
        return p -> {
            VariableReferenceExpression k = p.variable("k", BIGINT);
            VariableReferenceExpression arr = p.variable("arr", ARRAY_OF_BIGINT);
            VariableReferenceExpression elem = p.variable("elem", BIGINT);
            VariableReferenceExpression partitionKey = partitionOnUnnestedVariable ? elem : k;
            return p.exchange(e -> e
                    .type(REPARTITION)
                    .addSource(
                            p.project(
                                    Assignments.builder().put(k, k).put(elem, elem).build(),
                                    p.unnest(
                                            p.values(k, arr),
                                            ImmutableList.of(k),
                                            ImmutableMap.of(arr, ImmutableList.of(elem)),
                                            Optional.empty())))
                    .addInputsSet(k, elem)
                    .fixedHashDistributionPartitioningScheme(ImmutableList.of(k, elem), ImmutableList.of(partitionKey)));
        };
    }

    @Test
    public void testPullsChainAboveExchange()
    {
        // When enabled the rule fires whenever the rewrite is legal, independent of cost/statistics.
        tester().assertThat(new PullRowLocalChainAboveExchange(getFunctionManager()))
                .setSystemProperty(PULL_ROW_LOCAL_CHAIN_ABOVE_EXCHANGE_STRATEGY, "ALWAYS_ENABLED")
                .on(repartitionOverUnnest(false))
                .matches(
                        project(
                                unnest(
                                        exchange(
                                                values("k", "arr")))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PullRowLocalChainAboveExchange(getFunctionManager()))
                .on(repartitionOverUnnest(false))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenPartitionKeyIsUnnestedVariable()
    {
        // Partitioning on the unnested output (elem) cannot be pushed below the unnest.
        tester().assertThat(new PullRowLocalChainAboveExchange(getFunctionManager()))
                .setSystemProperty(PULL_ROW_LOCAL_CHAIN_ABOVE_EXCHANGE_STRATEGY, "ALWAYS_ENABLED")
                .on(repartitionOverUnnest(true))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNonDeterministicProjectionInChain()
    {
        // A non-deterministic projection between the exchange and the unnest is treated as a barrier,
        // so the chain (and the unnest below it) is never pulled across the exchange.
        tester().assertThat(new PullRowLocalChainAboveExchange(getFunctionManager()))
                .setSystemProperty(PULL_ROW_LOCAL_CHAIN_ABOVE_EXCHANGE_STRATEGY, "ALWAYS_ENABLED")
                .on(p -> {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression arr = p.variable("arr", ARRAY_OF_BIGINT);
                    VariableReferenceExpression elem = p.variable("elem", BIGINT);
                    VariableReferenceExpression rnd = p.variable("rnd", DOUBLE);
                    return p.exchange(e -> e
                            .type(REPARTITION)
                            .addSource(
                                    p.project(
                                            Assignments.builder()
                                                    .put(k, k)
                                                    .put(elem, elem)
                                                    .put(rnd, p.rowExpression("random()"))
                                                    .build(),
                                            p.unnest(
                                                    p.values(k, arr),
                                                    ImmutableList.of(k),
                                                    ImmutableMap.of(arr, ImmutableList.of(elem)),
                                                    Optional.empty())))
                            .addInputsSet(k, elem, rnd)
                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(k, elem, rnd), ImmutableList.of(k)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenExchangeRenamesOutput()
    {
        // The rule only handles single-source exchanges whose output layout equals their input (identity
        // mapping); a renaming exchange (output variables differ from the input) is left alone, because the
        // reparented chain would no longer produce the variables the consumer above the exchange expects.
        // In practice the planner does not emit a renaming SINGLE-source exchange (renaming only happens on
        // multi-source UNION exchanges, which are already excluded), so this locks in that defensive guard.
        // Here inputs = [k, elem] but the output layout is [k_out, elem_out], i.e. a rename.
        tester().assertThat(new PullRowLocalChainAboveExchange(getFunctionManager()))
                .setSystemProperty(PULL_ROW_LOCAL_CHAIN_ABOVE_EXCHANGE_STRATEGY, "ALWAYS_ENABLED")
                .on(p -> {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression arr = p.variable("arr", ARRAY_OF_BIGINT);
                    VariableReferenceExpression elem = p.variable("elem", BIGINT);
                    VariableReferenceExpression kOut = p.variable("k_out", BIGINT);
                    VariableReferenceExpression elemOut = p.variable("elem_out", BIGINT);
                    return p.exchange(e -> e
                            .type(REPARTITION)
                            .addSource(
                                    p.project(
                                            Assignments.builder().put(k, k).put(elem, elem).build(),
                                            p.unnest(
                                                    p.values(k, arr),
                                                    ImmutableList.of(k),
                                                    ImmutableMap.of(arr, ImmutableList.of(elem)),
                                                    Optional.empty())))
                            .addInputsSet(k, elem)
                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(kOut, elemOut), ImmutableList.of(kOut)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenNoUnnestInChain()
    {
        tester().assertThat(new PullRowLocalChainAboveExchange(getFunctionManager()))
                .setSystemProperty(PULL_ROW_LOCAL_CHAIN_ABOVE_EXCHANGE_STRATEGY, "ALWAYS_ENABLED")
                .on(p -> {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    VariableReferenceExpression v = p.variable("v", BIGINT);
                    return p.exchange(e -> e
                            .type(REPARTITION)
                            .addSource(
                                    p.project(
                                            Assignments.builder().put(k, k).put(v, v).build(),
                                            p.values(k, v)))
                            .addInputsSet(k, v)
                            .fixedHashDistributionPartitioningScheme(ImmutableList.of(k, v), ImmutableList.of(k)));
                })
                .doesNotFire();
    }
}
