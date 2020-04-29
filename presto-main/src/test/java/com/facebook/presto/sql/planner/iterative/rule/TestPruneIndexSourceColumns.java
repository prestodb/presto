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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.common.predicate.NullableValue.asNull;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedIndexSource;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestPruneIndexSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneIndexSourceColumns())
                .on(p -> buildProjectedIndexSource(p, variable -> variable.getName().equals("orderkey")))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", expression("orderkey")),
                                constrainedIndexSource(
                                        "orders",
                                        ImmutableMap.of("totalprice", Domain.onlyNull(DOUBLE)),
                                        ImmutableMap.of(
                                                "orderkey", "orderkey",
                                                "totalprice", "totalprice"))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneIndexSourceColumns())
                .on(p -> buildProjectedIndexSource(p, Predicates.alwaysTrue()))
                .doesNotFire();
    }

    private static PlanNode buildProjectedIndexSource(PlanBuilder p, Predicate<VariableReferenceExpression> projectionFilter)
    {
        VariableReferenceExpression orderkey = p.variable("orderkey", INTEGER);
        VariableReferenceExpression custkey = p.variable("custkey", INTEGER);
        VariableReferenceExpression totalprice = p.variable("totalprice", DOUBLE);
        ColumnHandle orderkeyHandle = new TpchColumnHandle(orderkey.getName(), INTEGER);
        ColumnHandle custkeyHandle = new TpchColumnHandle(custkey.getName(), INTEGER);
        ColumnHandle totalpriceHandle = new TpchColumnHandle(totalprice.getName(), DOUBLE);
        return p.project(
                identityAssignmentsAsSymbolReferences(
                        ImmutableList.of(orderkey, custkey, totalprice).stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.indexSource(
                        new TableHandle(
                                new ConnectorId("local"),
                                new TpchTableHandle("orders", TINY_SCALE_FACTOR),
                                TestingTransactionHandle.create(),
                                Optional.empty()),
                        ImmutableSet.of(orderkey, custkey),
                        ImmutableList.of(orderkey, custkey, totalprice),
                        ImmutableMap.of(
                                orderkey, orderkeyHandle,
                                custkey, custkeyHandle,
                                totalprice, totalpriceHandle),
                        TupleDomain.fromFixedValues(ImmutableMap.of(totalpriceHandle, asNull(DOUBLE)))));
    }
}
