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

package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCountConstantOptimizer
{
    @Test
    public void testCountConstantOptimizer()
            throws Exception
    {
        CountConstantOptimizer optimizer = new CountConstantOptimizer();
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        Symbol countAggregationSymbol = new Symbol("count");
        Signature countAggregationSignature = new Signature("count", FunctionKind.AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT));
        ImmutableMap<Symbol, FunctionCall> aggregations = ImmutableMap.of(countAggregationSymbol, new FunctionCall(QualifiedName.of("count"), ImmutableList.of(new SymbolReference("expr"))));
        ImmutableMap<Symbol, Signature> functions = ImmutableMap.of(countAggregationSymbol, countAggregationSignature);
        ValuesNode valuesNode = new ValuesNode(planNodeIdAllocator.getNextId(), ImmutableList.of(new Symbol("col")), ImmutableList.of(ImmutableList.of()));

        AggregationNode eligiblePlan = new AggregationNode(
                        planNodeIdAllocator.getNextId(),
                        new ProjectNode(
                                planNodeIdAllocator.getNextId(),
                                valuesNode,
                                Assignments.of(new Symbol("expr"), new LongLiteral("42"))),
                        aggregations,
                        functions,
                        ImmutableMap.of(),
                        ImmutableList.of(ImmutableList.of()),
                        AggregationNode.Step.INTERMEDIATE,
                        Optional.empty(),
                        Optional.empty());

        assertTrue(((AggregationNode) optimizer.optimize(eligiblePlan, TEST_SESSION, ImmutableMap.of(), new SymbolAllocator(), new PlanNodeIdAllocator()))
                .getAggregations()
                .get(countAggregationSymbol)
                .getArguments()
                .isEmpty());

        AggregationNode ineligiblePlan = new AggregationNode(
                        planNodeIdAllocator.getNextId(),
                        new ProjectNode(
                                planNodeIdAllocator.getNextId(),
                                valuesNode,
                                Assignments.of(new Symbol("expr"), new FunctionCall(QualifiedName.of("function"), ImmutableList.of(new QualifiedNameReference(QualifiedName.of("x")))))),
                        aggregations,
                        functions,
                        ImmutableMap.of(),
                        ImmutableList.of(ImmutableList.of()),
                        AggregationNode.Step.INTERMEDIATE,
                        Optional.empty(),
                        Optional.empty());

        assertFalse(((AggregationNode) optimizer.optimize(ineligiblePlan, TEST_SESSION, ImmutableMap.of(), new SymbolAllocator(), new PlanNodeIdAllocator()))
                .getAggregations()
                .get(countAggregationSymbol)
                .getArguments()
                .isEmpty());
    }
}
