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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

public class TestPlanClonerWithVariableMapping
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("test_catalog");
    private static final PlanNodeIdAllocator ID_ALLOCATOR = new PlanNodeIdAllocator();

    private VariableAllocator variableAllocator;

    @BeforeMethod
    public void setUp()
    {
        variableAllocator = new VariableAllocator();
    }

    @Test
    public void testCloneTableScanNode()
    {
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        VariableReferenceExpression totalprice = variable("totalprice", BIGINT);

        Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>();
        assignments.put(orderkey, new TestingColumnHandle("orderkey"));
        assignments.put(totalprice, new TestingColumnHandle("totalprice"));

        TableScanNode originalNode = new TableScanNode(
                Optional.empty(),
                new PlanNodeId("table_scan_1"),
                createTableHandle("orders"),
                ImmutableList.of(orderkey, totalprice),
                assignments,
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        TableScanNode clonedNode = (TableScanNode) cloned.getPlan();

        assertNotEquals(clonedNode.getId(), originalNode.getId());

        assertEquals(clonedNode.getOutputVariables().size(), 2);
        assertEquals(clonedNode.getOutputVariables().get(0).getName(), "orderkey");
        assertEquals(clonedNode.getOutputVariables().get(1).getName(), "totalprice");

        Map<VariableReferenceExpression, VariableReferenceExpression> mapping = cloned.getVariableMapping();
        assertEquals(mapping.size(), 2);
        assertEquals(mapping.get(orderkey).getName(), "orderkey");
        assertEquals(mapping.get(totalprice).getName(), "totalprice");
    }

    @Test
    public void testCloneProjectNode()
    {
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        VariableReferenceExpression doubled = variable("doubled", BIGINT);

        TableScanNode source = createTableScan(orderkey);

        RowExpression addExpression = new CallExpression(
                Optional.empty(),
                ADD.name(),
                mockFunctionHandle(),
                BIGINT,
                ImmutableList.of(orderkey, orderkey));

        Assignments assignments = Assignments.builder()
                .put(orderkey, orderkey)
                .put(doubled, addExpression)
                .build();

        ProjectNode originalNode = new ProjectNode(
                Optional.empty(),
                new PlanNodeId("project_1"),
                source,
                assignments,
                LOCAL);

        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        ProjectNode clonedNode = (ProjectNode) cloned.getPlan();

        assertNotEquals(clonedNode.getId(), originalNode.getId());
        assertEquals(clonedNode.getAssignments().getMap().size(), 2);
        assertTrue(clonedNode.getAssignments().getMap().keySet().stream()
                .allMatch(var -> var.getName().startsWith("orderkey") || var.getName().startsWith("totalprice") || var.getName().startsWith("doubled") || var.getName().startsWith("left_key") || var.getName().startsWith("right_key")));

        RowExpression clonedDoubledExpr = clonedNode.getAssignments().get(
                variable("doubled", BIGINT));
        assertTrue(clonedDoubledExpr instanceof CallExpression);
        CallExpression call = (CallExpression) clonedDoubledExpr;
        assertEquals(call.getArguments().size(), 2);
        assertEquals(((VariableReferenceExpression) call.getArguments().get(0)).getName(), "orderkey");
        assertEquals(((VariableReferenceExpression) call.getArguments().get(1)).getName(), "orderkey");

        assertNotSame(clonedNode.getSource(), source);
    }

    @Test
    public void testCloneFilterNode()
    {
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        TableScanNode source = createTableScan(orderkey);

        RowExpression predicate = new CallExpression(
                Optional.empty(),
                "greater_than",
                mockFunctionHandle(),
                BOOLEAN,
                ImmutableList.of(orderkey, constant(100L, BIGINT)));

        FilterNode originalNode = new FilterNode(
                Optional.empty(),
                new PlanNodeId("filter_1"),
                source,
                predicate);

        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        FilterNode clonedNode = (FilterNode) cloned.getPlan();

        assertNotEquals(clonedNode.getId(), originalNode.getId());

        assertTrue(clonedNode.getPredicate() instanceof CallExpression);
        CallExpression clonedPredicate = (CallExpression) clonedNode.getPredicate();
        assertEquals(((VariableReferenceExpression) clonedPredicate.getArguments().get(0)).getName(), "orderkey");
    }

    @Test
    public void testCloneJoinNode()
    {
        VariableReferenceExpression leftKey = variable("left_key", BIGINT);
        VariableReferenceExpression rightKey = variable("right_key", BIGINT);

        TableScanNode leftSource = createTableScan(leftKey);
        TableScanNode rightSource = createTableScan(rightKey);

        EquiJoinClause criteria = new EquiJoinClause(leftKey, rightKey);

        JoinNode originalNode = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join_1"),
                JoinType.INNER,
                leftSource,
                rightSource,
                ImmutableList.of(criteria),
                ImmutableList.of(leftKey, rightKey),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        JoinNode clonedNode = (JoinNode) cloned.getPlan();

        assertNotEquals(clonedNode.getId(), originalNode.getId());

        assertEquals(clonedNode.getCriteria().size(), 1);
        EquiJoinClause clonedCriteria = clonedNode.getCriteria().get(0);
        assertEquals(clonedCriteria.getLeft().getName(), "left_key");
        assertEquals(clonedCriteria.getRight().getName(), "right_key");

        assertEquals(clonedNode.getOutputVariables().size(), 2);
        assertTrue(clonedNode.getOutputVariables().stream()
                .allMatch(var -> var.getName().startsWith("orderkey") || var.getName().startsWith("totalprice") || var.getName().startsWith("doubled") || var.getName().startsWith("left_key") || var.getName().startsWith("right_key")));

        assertNotSame(clonedNode.getLeft(), leftSource);
        assertNotSame(clonedNode.getRight(), rightSource);
    }

    @Test
    public void testMultipleClonesHaveIndependentVariables()
    {
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        TableScanNode originalNode = createTableScan(orderkey);

        PlanClonerWithVariableMapping.ClonedPlan clone1 = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);
        PlanClonerWithVariableMapping.ClonedPlan clone2 = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);
        PlanClonerWithVariableMapping.ClonedPlan clone3 = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        TableScanNode node1 = (TableScanNode) clone1.getPlan();
        TableScanNode node2 = (TableScanNode) clone2.getPlan();
        TableScanNode node3 = (TableScanNode) clone3.getPlan();

        assertEquals(node1.getOutputVariables().get(0).getName(), "orderkey");
        assertEquals(node2.getOutputVariables().get(0).getName(), "orderkey_0");
        assertEquals(node3.getOutputVariables().get(0).getName(), "orderkey_1");

        assertNotEquals(node1.getId(), node2.getId());
        assertNotEquals(node2.getId(), node3.getId());
        assertNotEquals(node1.getId(), node3.getId());
    }

    @Test
    public void testNestedPlanCloning()
    {
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);

        TableScanNode scan = createTableScan(orderkey);

        FilterNode filter = new FilterNode(
                Optional.empty(),
                new PlanNodeId("filter"),
                scan,
                new CallExpression(
                        Optional.empty(),
                        "greater_than",
                        mockFunctionHandle(),
                        BOOLEAN,
                        ImmutableList.of(orderkey, constant(100L, BIGINT))));

        ProjectNode project = new ProjectNode(
                Optional.empty(),
                new PlanNodeId("project"),
                filter,
                Assignments.builder().put(orderkey, orderkey).build(),
                LOCAL);

        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(project, variableAllocator, ID_ALLOCATOR);

        ProjectNode clonedProject = (ProjectNode) cloned.getPlan();
        FilterNode clonedFilter = (FilterNode) clonedProject.getSource();
        TableScanNode clonedScan = (TableScanNode) clonedFilter.getSource();

        assertNotEquals(clonedProject.getId(), project.getId());
        assertNotEquals(clonedFilter.getId(), filter.getId());
        assertNotEquals(clonedScan.getId(), scan.getId());

        assertEquals(clonedProject.getOutputVariables().get(0).getName(), "orderkey");
        CallExpression predicate = (CallExpression) clonedFilter.getPredicate();
        assertEquals(((VariableReferenceExpression) predicate.getArguments().get(0)).getName(), "orderkey");
        assertEquals(clonedScan.getOutputVariables().get(0).getName(), "orderkey");
    }

    @Test
    public void testVariableMappingCompleteness()
    {
        VariableReferenceExpression var1 = variable("var1", BIGINT);
        VariableReferenceExpression var2 = variable("var2", BIGINT);
        VariableReferenceExpression var3 = variable("var3", BIGINT);

        Map<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.of(
                var1, new TestingColumnHandle("col1"),
                var2, new TestingColumnHandle("col2"),
                var3, new TestingColumnHandle("col3"));

        TableScanNode node = new TableScanNode(
                Optional.empty(),
                new PlanNodeId("scan"),
                createTableHandle("table"),
                ImmutableList.of(var1, var2, var3),
                assignments,
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(node, variableAllocator, ID_ALLOCATOR);

        Map<VariableReferenceExpression, VariableReferenceExpression> mapping = cloned.getVariableMapping();
        assertEquals(mapping.size(), 3);
        assertTrue(mapping.containsKey(var1));
        assertTrue(mapping.containsKey(var2));
        assertTrue(mapping.containsKey(var3));
        assertEquals(mapping.get(var1).getName(), "var1");
        assertEquals(mapping.get(var2).getName(), "var2");
        assertEquals(mapping.get(var3).getName(), "var3");
    }

    // Helper methods

    private static VariableReferenceExpression variable(String name, Type type)
    {
        return new VariableReferenceExpression(Optional.empty(), name, type);
    }

    private static ConstantExpression constant(Object value, Type type)
    {
        return new ConstantExpression(Optional.empty(), value, type);
    }

    private static TableScanNode createTableScan(VariableReferenceExpression... variables)
    {
        Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>();
        for (VariableReferenceExpression var : variables) {
            assignments.put(var, new TestingColumnHandle(var.getName()));
        }

        return new TableScanNode(
                Optional.empty(),
                ID_ALLOCATOR.getNextId(),
                createTableHandle("test_table"),
                ImmutableList.copyOf(variables),
                assignments,
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }

    private static TableHandle createTableHandle(String tableName)
    {
        return new TableHandle(
                CONNECTOR_ID,
                new TestingTableHandle(new SchemaTableName("test_schema", tableName)),
                mockTransactionHandle(),
                Optional.empty());
    }

    private static ConnectorTransactionHandle mockTransactionHandle()
    {
        // Create a mock transaction handle for testing
        return new ConnectorTransactionHandle()
        {
            @Override
            public String toString()
            {
                return "mock-transaction-handle";
            }
        };
    }

    private static com.facebook.presto.spi.function.FunctionHandle mockFunctionHandle()
    {
        // Create a mock function handle for testing
        return new com.facebook.presto.spi.function.FunctionHandle()
        {
            @Override
            public CatalogSchemaName getCatalogSchemaName()
            {
                return new CatalogSchemaName("test_catalog", "test_schema");
            }

            @Override
            public String getName()
            {
                return "test_function";
            }

            @Override
            public com.facebook.presto.spi.function.FunctionKind getKind()
            {
                return com.facebook.presto.spi.function.FunctionKind.SCALAR;
            }

            @Override
            public java.util.List<TypeSignature> getArgumentTypes()
            {
                return ImmutableList.of();
            }
        };
    }
}
