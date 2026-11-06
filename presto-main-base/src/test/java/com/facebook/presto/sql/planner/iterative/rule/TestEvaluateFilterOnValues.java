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

import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.EVALUATE_FILTER_ON_VALUES;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static io.airlift.slice.Slices.utf8Slice;

public class TestEvaluateFilterOnValues
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(rule())
                .setSystemProperty(EVALUATE_FILTER_ON_VALUES, "false")
                .on(p -> filterOnKeyDsValues(p, "ds = 'b'"))
                .doesNotFire();
    }

    @Test
    public void testRemovesAllRows()
    {
        tester().assertThat(rule())
                .setSystemProperty(EVALUATE_FILTER_ON_VALUES, "true")
                .on(p -> filterOnKeyDsValues(p, "ds = 'z'"))
                .matches(values(ImmutableList.of("k", "ds"), ImmutableList.of()));
    }

    @Test
    public void testRemovesSomeRows()
    {
        tester().assertThat(rule())
                .setSystemProperty(EVALUATE_FILTER_ON_VALUES, "true")
                .on(p -> filterOnKeyDsValues(p, "k > 1 AND ds <> 'c'"))
                .matches(values(
                        ImmutableList.of("k", "ds"),
                        ImmutableList.of(ImmutableList.of(new LongLiteral("2"), new StringLiteral("b")))));
    }

    @Test
    public void testRemovesRowsWithNullPredicate()
    {
        tester().assertThat(rule())
                .setSystemProperty(EVALUATE_FILTER_ON_VALUES, "true")
                .on(p -> {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    return p.filter(
                            p.rowExpression("k = 1"),
                            p.values(
                                    ImmutableList.of(k),
                                    ImmutableList.of(
                                            ImmutableList.of(constant(1L, BIGINT)),
                                            ImmutableList.of(constantNull(BIGINT)))));
                })
                .matches(values(
                        ImmutableList.of("k"),
                        ImmutableList.of(ImmutableList.of(new LongLiteral("1")))));
    }

    @Test
    public void testDoesNotFireOnNonDeterministicPredicate()
    {
        tester().assertThat(rule())
                .setSystemProperty(EVALUATE_FILTER_ON_VALUES, "true")
                .on(p -> filterOnKeyDsValues(p, "k > random(10)"))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonDeterministicRow()
    {
        tester().assertThat(rule())
                .setSystemProperty(EVALUATE_FILTER_ON_VALUES, "true")
                .on(p -> {
                    VariableReferenceExpression k = p.variable("k", BIGINT);
                    return p.filter(
                            p.rowExpression("k > 1"),
                            p.values(ImmutableList.of(k), ImmutableList.of(ImmutableList.of(p.rowExpression("random(10)")))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenPredicateDoesNotFold()
    {
        // 1 / (k - 1) fails for k = 1, so the filter is kept and the failure is raised at execution time
        tester().assertThat(rule())
                .setSystemProperty(EVALUATE_FILTER_ON_VALUES, "true")
                .on(p -> filterOnKeyDsValues(p, "1 / (k - 1) > 0"))
                .doesNotFire();
    }

    private EvaluateFilterOnValues rule()
    {
        return new EvaluateFilterOnValues(getMetadata().getFunctionAndTypeManager(), getExpressionManager());
    }

    private static RowExpression varchar(String value)
    {
        return constant(utf8Slice(value), VARCHAR);
    }

    private static FilterNode filterOnKeyDsValues(PlanBuilder p, String predicate)
    {
        // Create the values first so the predicate can resolve the types of its variables
        ValuesNode values = keyDsValues(p);
        return p.filter(p.rowExpression(predicate), values);
    }

    private static ValuesNode keyDsValues(PlanBuilder p)
    {
        VariableReferenceExpression k = p.variable("k", BIGINT);
        VariableReferenceExpression ds = p.variable("ds", VARCHAR);
        List<List<RowExpression>> rows = ImmutableList.of(
                ImmutableList.of(constant(1L, BIGINT), varchar("a")),
                ImmutableList.of(constant(2L, BIGINT), varchar("b")),
                ImmutableList.of(constant(3L, BIGINT), varchar("c")));
        return p.values(ImmutableList.of(k, ds), rows);
    }
}
