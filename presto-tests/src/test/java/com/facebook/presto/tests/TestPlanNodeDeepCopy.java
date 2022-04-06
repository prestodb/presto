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
package com.facebook.presto.tests;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.CanonicalTableScanNode;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

public class TestPlanNodeDeepCopy
{
    private LocalQueryRunner queryRunner;
    PlanNodeIdAllocator planNodeIdAllocator;
    Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings;
    PlanBuilder p;
    PlanNodeDeepCopyChecker planNodeDeepCopyChecker;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(TEST_SESSION);
        planNodeIdAllocator = new PlanNodeIdAllocator();
        variableMappings = new HashMap<>();
//        Session tpchSession = testSessionBuilder()
//            .setCatalog("tpch")
//            .setSchema(TINY_SCHEMA_NAME)
//            .build();
        p = new PlanBuilder(TEST_SESSION, planNodeIdAllocator, queryRunner.getMetadata());
        planNodeDeepCopyChecker = new PlanNodeDeepCopyChecker(variableMappings);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner = null;
        planNodeIdAllocator = null;
        variableMappings = null;
        p = null;
        planNodeDeepCopyChecker = null;
    }

    public ConnectorId getCurrentConnectorId()
    {
        return queryRunner.inTransaction(transactionSession -> queryRunner.getMetadata().getCatalogHandle(transactionSession, transactionSession.getCatalog().get())).get();
    }

    @Test
    public void test()
    {
        Plan queryPlan = queryRunner.createPlan(TEST_SESSION, "SELECT * FROM (VALUES 1, 2, 3) t(x) WHERE t.x IN (SELECT * FROM (VALUES 1,2,3))", LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, WarningCollector.NOOP);
        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        PlanNode node = new CanonicalTableScanNode(
                Optional.empty(),
                planNodeIdAllocator.getNextId(),
                new CanonicalTableScanNode.CanonicalTableHandle(
                        new ConnectorId("tpch"),
                        new TpchTableHandle("nation", 1.0),
                        Optional.of(new TpchTableLayoutHandle(nationTpchTableHandle, TupleDomain.all()))),
                ImmutableList.of(p.variable("nationkey", BIGINT)),
                ImmutableMap.of(p.variable("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)));
        VariableAllocator variableAllocator = new PlanVariableAllocator(p.getTypes().allVariables());
        planNodeDeepCopyChecker.validate(node, node.deepCopy(planNodeIdAllocator, variableAllocator, variableMappings));
    }

    @Test
    public void testCanonicalTableScanNode()
    {
        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        PlanNode node = new CanonicalTableScanNode(
                Optional.empty(),
                planNodeIdAllocator.getNextId(),
                new CanonicalTableScanNode.CanonicalTableHandle(
                        new ConnectorId("tpch"),
                        new TpchTableHandle("nation", 1.0),
                        Optional.of(new TpchTableLayoutHandle(nationTpchTableHandle, TupleDomain.all()))),
                ImmutableList.of(p.variable("nationkey", BIGINT)),
                ImmutableMap.of(p.variable("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)));
        VariableAllocator variableAllocator = new PlanVariableAllocator(p.getTypes().allVariables());
        planNodeDeepCopyChecker.validate(node, node.deepCopy(planNodeIdAllocator, variableAllocator, variableMappings));
    }

    @Test
    public void testAssignUniqueId()
    {
        PlanNode node = p.assignUniqueId(p.variable("id"), p.values(p.variable("col1"), p.variable("col2")));
        VariableAllocator variableAllocator = new PlanVariableAllocator(p.getTypes().allVariables());
        planNodeDeepCopyChecker.validate(node, node.deepCopy(planNodeIdAllocator, variableAllocator, variableMappings));
    }

    @Test
    public void testSortNode()
    {
        VariableReferenceExpression a = p.variable("a");
        VariableReferenceExpression b = p.variable("b");
        PlanNode node = p.sort(
                ImmutableList.of(a),
                p.values(a, b));
        VariableAllocator variableAllocator = new PlanVariableAllocator(p.getTypes().allVariables());
        planNodeDeepCopyChecker.validate(node, node.deepCopy(planNodeIdAllocator, variableAllocator, variableMappings));
    }

    @Test
    public void testExchangeNode()
    {
        PlanNode node = p.exchange(exchangeBuilder -> exchangeBuilder
                .addInputsSet(p.variable("i11", BIGINT), p.variable("i12", BIGINT), p.variable("i13", BIGINT), p.variable("i14", BIGINT))
                .addInputsSet(p.variable("i21", BIGINT), p.variable("i22", BIGINT), p.variable("i23", BIGINT), p.variable("i24", BIGINT))
                .fixedHashDistributionPartitioningScheme(
                        ImmutableList.of(p.variable("o1", BIGINT), p.variable("o2", BIGINT), p.variable("o3", BIGINT), p.variable("o4", BIGINT)),
                        emptyList())
                .addSource(p.values(p.variable("i11", BIGINT), p.variable("i12", BIGINT), p.variable("i13", BIGINT), p.variable("i14", BIGINT)))
                .addSource(p.values(p.variable("i21", BIGINT), p.variable("i22", BIGINT), p.variable("i23", BIGINT), p.variable("i24", BIGINT))));
        VariableAllocator variableAllocator = new PlanVariableAllocator(p.getTypes().allVariables());
        planNodeDeepCopyChecker.validate(node, node.deepCopy(planNodeIdAllocator, variableAllocator, variableMappings));
    }

    @Test
    public void testApplyNode()
    {
        PlanNode node = p.apply(
                assignment(
                        p.variable("a", BIGINT),
                        new InPredicate(
                                new LongLiteral("1"),
                                new InListExpression(ImmutableList.of(
                                        new LongLiteral("1"),
                                        new LongLiteral("2"))))),
                ImmutableList.of(),
                p.values(3, p.variable("input_field")),
                p.values(3, p.variable("subquery_field")));
        VariableAllocator variableAllocator = new PlanVariableAllocator(p.getTypes().allVariables());
        planNodeDeepCopyChecker.validate(node, node.deepCopy(planNodeIdAllocator, variableAllocator, variableMappings));
    }

    @Test
    public void testValuesNode()
    {
        Plan queryPlan = queryRunner.createPlan(TEST_SESSION, "SELECT * FROM (VALUES 1, 2, 3) t(x)", WarningCollector.NOOP);
        PlanNode node = p.values(3, p.variable("field"));
        VariableAllocator variableAllocator = new PlanVariableAllocator(p.getTypes().allVariables());
        planNodeDeepCopyChecker.validate(node, node.deepCopy(planNodeIdAllocator, variableAllocator, variableMappings));
    }

    @Test
    public void test1()
    {
        Plan queryPlan = queryRunner.createPlan(TEST_SESSION, "SELECT * FROM (VALUES 1, 2, 3) t(x)", WarningCollector.NOOP);

        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new PlanVariableAllocator(queryPlan.getTypes().allVariables());
        Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings = new HashMap<>();
        PlanNode rootNodeCopy = queryPlan.getRoot().deepCopy(planNodeIdAllocator, variableAllocator, variableMappings);
        PlanNodeDeepCopyChecker planNodeDeepCopyChecker = new PlanNodeDeepCopyChecker(variableMappings);
        planNodeDeepCopyChecker.validate(queryPlan.getRoot(), rootNodeCopy);
    }

    public static class PlanNodeDeepCopyChecker
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings;
        PlanNodeDeepCopyChecker(Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings)
        {
            this.variableMappings = variableMappings;
        }
        public void validate(PlanNode original, PlanNode copy)
        {
            assertEquals(original.getClass(), copy.getClass(), String.format("'%s' type of the plan node does not match the type '%s' of original node", copy.getClass(), original.getClass()));
            assertNotSame(original.getId(), copy.getId());
            assertEquals(original.getSourceLocation(), copy.getSourceLocation());
            assertVariablesDeepCopied(original.getOutputVariables(), copy.getOutputVariables());
            if (copy instanceof OutputNode) {
                OutputNode originalOutputNode = (OutputNode) original;
                OutputNode copyOutputNode = (OutputNode) copy;
                assertNotSame(originalOutputNode, copyOutputNode);
                validate(originalOutputNode.getSource(), ((OutputNode) copy).getSource());
                assertEquals(originalOutputNode.getColumnNames(), copyOutputNode.getColumnNames());
            }
            else if (copy instanceof ValuesNode) {
                ValuesNode originalValuesNode = (ValuesNode) original;
                ValuesNode copyValuesNode = (ValuesNode) copy;
                assertNotSame(originalValuesNode, copyValuesNode, "Two nodes are pointing to the same heap location and thus not copied");
                for (int row = 0; row < originalValuesNode.getRows().size(); ++row) {
                    for (int col = 0; col < originalValuesNode.getRows().get(row).size(); ++col) {
                        assertNotSame(originalValuesNode.getRows().get(row).get(col), copyValuesNode.getRows().get(row).get(col));
                        assertEquals(originalValuesNode.getRows().get(row).get(col), copyValuesNode.getRows().get(row).get(col));
                    }
                }
            }
            else if (copy instanceof CanonicalTableScanNode) {
                CanonicalTableScanNode originalCanonicalTableScanNode = (CanonicalTableScanNode) original;
                CanonicalTableScanNode copyCanonicalTableScanNode = (CanonicalTableScanNode) copy;
                assertNotSame(originalCanonicalTableScanNode, copyCanonicalTableScanNode);
                assertEquals(originalCanonicalTableScanNode.getTable(), copyCanonicalTableScanNode.getTable());
                for (VariableReferenceExpression originalVariable : originalCanonicalTableScanNode.getAssignments().keySet()) {
                    assertEquals(
                            originalCanonicalTableScanNode.getAssignments().get(originalVariable),
                            copyCanonicalTableScanNode.getAssignments().get(variableMappings.get(originalVariable)));
                }
            }
            else if (copy instanceof AssignUniqueId) {
                AssignUniqueId originalAssignUniqueId = (AssignUniqueId) original;
                AssignUniqueId copyAssignUniqueId = (AssignUniqueId) copy;
                assertNotSame(originalAssignUniqueId, copyAssignUniqueId);
                assertRowExpressionDeepCopied(originalAssignUniqueId.getIdVariable(), copyAssignUniqueId.getIdVariable());
                validate(originalAssignUniqueId.getSource(), copyAssignUniqueId.getSource());
            }
            else if (copy instanceof SortNode) {
                SortNode originalSortNode = (SortNode) original;
                SortNode copySortNode = (SortNode) copy;
                assertNotSame(originalSortNode, copySortNode);
                assertEquals(originalSortNode.isPartial(), copySortNode.isPartial());
                assertOrderingSchemeDeepCopied(originalSortNode.getOrderingScheme(), copySortNode.getOrderingScheme());

                validate(originalSortNode.getSource(), copySortNode.getSource());
            }
            else if (copy instanceof ApplyNode) {
                ApplyNode originalApplyNode = (ApplyNode) original;
                ApplyNode copyApplyNode = (ApplyNode) copy;
                assertNotSame(originalApplyNode, copyApplyNode, "Two nodes are pointing to the same heap location and thus not copied");
                validate(originalApplyNode.getInput(), copyApplyNode.getInput());
                validate(originalApplyNode.getSubquery(), copyApplyNode.getSubquery());
                assertAssignmentsDeepCopied(originalApplyNode.getSubqueryAssignments(), copyApplyNode.getSubqueryAssignments());
            }
            else if (copy instanceof ExchangeNode) {
                ExchangeNode originalExchangeNode = (ExchangeNode) original;
                ExchangeNode copyExchangeNode = (ExchangeNode) copy;
                assertNotSame(originalExchangeNode, copyExchangeNode);
                assertEquals(originalExchangeNode.getType(), copyExchangeNode.getType());
                assertEquals(originalExchangeNode.getScope(), copyExchangeNode.getScope());
                assertEquals(originalExchangeNode.getScope(), copyExchangeNode.getScope());
                assertPlanNodesDeepCopied(originalExchangeNode.getSources(), copyExchangeNode.getSources());
                assertEquals(
                        copyExchangeNode.getInputs(),
                        originalExchangeNode.getInputs().stream()
                                .map(variables -> variables.stream()
                                        .map(v -> variableMappings.get(v))
                                        .collect(collectingAndThen(toList(), ImmutableList::copyOf)))
                                .collect(collectingAndThen(toList(), ImmutableList::copyOf)));
                assertEquals(originalExchangeNode.isEnsureSourceOrdering(), copyExchangeNode.isEnsureSourceOrdering());
                assertEquals(originalExchangeNode.getOrderingScheme().isPresent(), copyExchangeNode.getOrderingScheme().isPresent());
                if (originalExchangeNode.getOrderingScheme().isPresent()) {
                    assertOrderingSchemeDeepCopied(originalExchangeNode.getOrderingScheme().get(), originalExchangeNode.getOrderingScheme().get());
                }
            }
        }

        private void assertOrderingSchemeDeepCopied(OrderingScheme original, OrderingScheme copy)
        {
            for (VariableReferenceExpression variable : original.getOrderingsMap().keySet()) {
                assertEquals(original.getOrdering(variable), copy.getOrdering(variableMappings.get(variable)));
            }
        }

        static void assertStreamEquals(Stream<?> s1, Stream<?> s2)
        {
            Iterator<?> it1 = s1.iterator();
            Iterator<?> it2 = s2.iterator();
            while (it1.hasNext() && it2.hasNext()) {
                assertEquals(it1.next(), it2.next());
            }
            assertTrue(!it1.hasNext() && !it2.hasNext());
        }

        private void assertVariablesDeepCopied(List<VariableReferenceExpression> original, List<VariableReferenceExpression> copy)
        {
            assertEquals(original.size(), copy.size());
            for (int i = 0; i < original.size(); ++i) {
                assertNotSame(original.get(i), copy.get(i));
                assertEquals(variableMappings.get(original.get(i)), copy.get(i));
            }
        }

        private void assertAssignmentsDeepCopied(Assignments original, Assignments copy)
        {
            assertNotSame(original, copy);
            assertVariablesDeepCopied(original.getOutputs(), copy.getOutputs());
            assertEquals(original.getVariables().size(), copy.getVariables().size());
            for (VariableReferenceExpression key : original.getVariables()) {
                assertEquals(copy.getMap().containsKey(variableMappings.get(key)), true);
                assertRowExpressionDeepCopied(original.get(key), copy.get(variableMappings.get(key)));
            }
        }

        private void assertPlanNodesDeepCopied(List<PlanNode> original, List<PlanNode> copy)
        {
            if (original != copy) {
                if (original == null || copy == null) {
                    if (copy != null) {
                        throw new AssertionError();
                    }
                    else {
                        throw new AssertionError("Plan node lists are not equal: " + original + " and " + copy);
                    }
                }
                if (original.size() != copy.size()) {
                    throw new AssertionError("Plan node list sizes are not equal: " + original.size() + " and " + copy.size());
                }
                for (int i = 0; i < original.size(); ++i) {
                    validate(original.get(i), copy.get(i));
                }
            }
        }
        private void assertRowExpressionListDeepCopied(List<RowExpression> original, List<RowExpression> copy)
        {
            if (original != copy) {
                if (original == null || copy == null) {
                    if (copy != null) {
                        throw new AssertionError();
                    }
                    else {
                        throw new AssertionError("Expressions are not equal: " + original + " and " + copy);
                    }
                }
                if (original.size() != copy.size()) {
                    throw new AssertionError("Expression list sizes are not equal: " + original.size() + " and " + copy.size());
                }
                for (int i = 0; i < original.size(); ++i) {
                    assertRowExpressionDeepCopied(original.get(i), copy.get(i));
                }
            }
        }

        private void assertRowExpressionDeepCopied(RowExpression original, RowExpression copy)
        {
            assertEquals(original.getClass(), copy.getClass(), String.format("'%s' type of the expression does not match the type '%s' of original expression", copy.getClass(), original.getClass()));
            assertEquals(original.getSourceLocation(), copy.getSourceLocation());
            assertNotSame(original, copy);
            if (original instanceof CallExpression) {
                CallExpression originalCallExpression = (CallExpression) original;
                CallExpression copyCallExpression = (CallExpression) copy;
                assertNotSame(originalCallExpression, copyCallExpression);
                assertEquals(originalCallExpression.getDisplayName(), copyCallExpression.getDisplayName());
                assertEquals(originalCallExpression.getFunctionHandle(), copyCallExpression.getFunctionHandle());
                assertEquals(originalCallExpression.getType(), copyCallExpression.getType());
                assertEquals(originalCallExpression.getArguments(), copyCallExpression.getArguments());
            }
            else if (original instanceof ConstantExpression) {
                ConstantExpression originalConstantExpression = (ConstantExpression) original;
                ConstantExpression copyConstantExpression = (ConstantExpression) copy;
                assertEquals(originalConstantExpression.getType(), copyConstantExpression.getType());
                assertEquals(originalConstantExpression.getValue(), copyConstantExpression.getValue());
            }
            else if (original instanceof InputReferenceExpression) {
                InputReferenceExpression originalInputReferenceExpression = (InputReferenceExpression) original;
                InputReferenceExpression copyInputReferenceExpression = (InputReferenceExpression) copy;
                assertEquals(originalInputReferenceExpression.getType(), copyInputReferenceExpression.getType());
                assertEquals(originalInputReferenceExpression.getField(), copyInputReferenceExpression.getField());
            }
            else if (original instanceof LambdaDefinitionExpression) {
                LambdaDefinitionExpression originalLambdaDefinitionExpression = (LambdaDefinitionExpression) original;
                LambdaDefinitionExpression copyLambdaDefinitionExpression = (LambdaDefinitionExpression) copy;
                assertEquals(originalLambdaDefinitionExpression.getType(), copyLambdaDefinitionExpression.getType());
                assertEquals(originalLambdaDefinitionExpression.getArguments(), copyLambdaDefinitionExpression.getArguments());
                assertEquals(originalLambdaDefinitionExpression.getArgumentTypes(), copyLambdaDefinitionExpression.getArgumentTypes());
                assertRowExpressionDeepCopied(originalLambdaDefinitionExpression.getBody(), copyLambdaDefinitionExpression.getBody());
            }
            else if (original instanceof SpecialFormExpression) {
                SpecialFormExpression originalSpecialFormExpression = (SpecialFormExpression) original;
                SpecialFormExpression copySpecialFormExpression = (SpecialFormExpression) copy;
                assertEquals(originalSpecialFormExpression.getType(), copySpecialFormExpression.getType());
                assertEquals(originalSpecialFormExpression.getForm(), copySpecialFormExpression.getForm());
                assertEquals(originalSpecialFormExpression.getArguments().size(), copySpecialFormExpression.getArguments().size());
                for (int i = 0; i < originalSpecialFormExpression.getArguments().size(); ++i) {
                    assertRowExpressionDeepCopied(originalSpecialFormExpression.getArguments().get(i), copySpecialFormExpression.getArguments().get(i));
                }
            }
            else if (original instanceof VariableReferenceExpression) {
                VariableReferenceExpression originalVariableReferenceExpression = (VariableReferenceExpression) original;
                VariableReferenceExpression copyVariableReferenceExpression = (VariableReferenceExpression) copy;
                assertEquals(originalVariableReferenceExpression.getType(), copyVariableReferenceExpression.getType());
                assertEquals(variableMappings.get(originalVariableReferenceExpression), copyVariableReferenceExpression);
            }
        }
    }
}
