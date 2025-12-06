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

import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.planner.sanity.TypeValidator;
import com.facebook.presto.sql.planner.sanity.ValidateDependenciesChecker;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPlanClonerWithVariableMapping
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("test_catalog");
    private static final PlanNodeIdAllocator ID_ALLOCATOR = new PlanNodeIdAllocator();

    private VariableAllocator variableAllocator;
    private Session session;
    private FunctionAndTypeManager functionAndTypeManager;

    @BeforeMethod
    public void setUp()
    {
        variableAllocator = new VariableAllocator();
        session = TestingSession.testSessionBuilder().build();
        functionAndTypeManager = createTestFunctionAndTypeManager();
    }

    @Test
    public void testCloneTableScanNode()
    {
        // Given: A simple TableScanNode
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

        // When: Clone the plan
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testCloneProjectNode()
    {
        // Given: ProjectNode with expression
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        VariableReferenceExpression doubled = variable("doubled", BIGINT);

        TableScanNode source = createTableScan(orderkey);

        // doubled = orderkey + orderkey
        RowExpression addExpression = new CallExpression(
                Optional.empty(),
                ADD.name(),
                functionAndTypeManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
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

        // When: Clone the plan
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testCloneFilterNode()
    {
        // Given: FilterNode with predicate
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        TableScanNode source = createTableScan(orderkey);

        // predicate: orderkey > 100
        RowExpression predicate = new CallExpression(
                Optional.empty(),
                GREATER_THAN.name(),
                functionAndTypeManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                ImmutableList.of(orderkey, constant(100L, BIGINT)));

        FilterNode originalNode = new FilterNode(
                Optional.empty(),
                new PlanNodeId("filter_1"),
                source,
                predicate);

        // When: Clone the plan
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testCloneJoinNode()
    {
        // Given: JoinNode with criteria
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

        // When: Clone the plan
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testMultipleClonesHaveIndependentVariables()
    {
        // Given: A simple plan
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        TableScanNode originalNode = createTableScan(orderkey);

        // When: Clone multiple times (VariableAllocator ensures uniqueness)
        PlanClonerWithVariableMapping.ClonedPlan clone1 = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);
        PlanClonerWithVariableMapping.ClonedPlan clone2 = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);
        PlanClonerWithVariableMapping.ClonedPlan clone3 = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Each clone should have different variable names (VariableAllocator adds _0, _1, _2, etc.)
        TableScanNode node1 = (TableScanNode) clone1.getPlan();
        TableScanNode node2 = (TableScanNode) clone2.getPlan();
        TableScanNode node3 = (TableScanNode) clone3.getPlan();

        assertEquals(node1.getOutputVariables().get(0).getName(), "orderkey");
        assertEquals(node2.getOutputVariables().get(0).getName(), "orderkey_0");
        assertEquals(node3.getOutputVariables().get(0).getName(), "orderkey_1");

        // And: Each clone should have different node IDs
        assertNotEquals(node1.getId(), node2.getId());
        assertNotEquals(node2.getId(), node3.getId());
        assertNotEquals(node1.getId(), node3.getId());
    }

    @Test
    public void testNestedPlanCloning()
    {
        // Given: Nested plan (Project -> Filter -> TableScan)
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);

        TableScanNode scan = createTableScan(orderkey);

        FilterNode filter = new FilterNode(
                Optional.empty(),
                new PlanNodeId("filter"),
                scan,
                new CallExpression(
                        Optional.empty(),
                        GREATER_THAN.name(),
                        functionAndTypeManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                        BOOLEAN,
                        ImmutableList.of(orderkey, constant(100L, BIGINT))));

        ProjectNode project = new ProjectNode(
                Optional.empty(),
                new PlanNodeId("project"),
                filter,
                Assignments.builder().put(orderkey, orderkey).build(),
                LOCAL);

        // When: Clone the nested plan
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(project, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(project, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testVariableMappingCompleteness()
    {
        // Given: A plan with multiple variables
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

        // When: Clone the plan
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(node, variableAllocator, ID_ALLOCATOR);

        // Then: Variable mapping should include all original variables
        Map<VariableReferenceExpression, VariableReferenceExpression> mapping = cloned.getVariableMapping();
        assertEquals(mapping.size(), 3);
        assertTrue(mapping.containsKey(var1));
        assertTrue(mapping.containsKey(var2));
        assertTrue(mapping.containsKey(var3));
        assertEquals(mapping.get(var1).getName(), "var1");
        assertEquals(mapping.get(var2).getName(), "var2");
        assertEquals(mapping.get(var3).getName(), "var3");
    }

    @Test
    public void testCloneLimitNode()
    {
        // Given: A simple LimitNode
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        TableScanNode source = createTableScan(orderkey);

        LimitNode originalNode = new LimitNode(
                Optional.empty(),
                new PlanNodeId("limit_1"),
                source,
                10L,
                LimitNode.Step.FINAL);

        // When: Clone the plan
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testCloneJoinNodeWithFilter()
    {
        // Given: JoinNode with an optional filter expression
        VariableReferenceExpression leftKey = variable("left_key", BIGINT);
        VariableReferenceExpression rightKey = variable("right_key", BIGINT);
        VariableReferenceExpression leftValue = variable("left_value", BIGINT);

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = ImmutableMap.of(
                leftKey, new TestingColumnHandle("left_key"),
                leftValue, new TestingColumnHandle("left_value"));

        TableScanNode leftSource = new TableScanNode(
                Optional.empty(),
                ID_ALLOCATOR.getNextId(),
                createTableHandle("left_table"),
                ImmutableList.of(leftKey, leftValue),
                leftAssignments,
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        TableScanNode rightSource = createTableScan(rightKey);

        EquiJoinClause criteria = new EquiJoinClause(leftKey, rightKey);

        // Filter: left_value > 100
        RowExpression filterExpression = new CallExpression(
                Optional.empty(),
                GREATER_THAN.name(),
                functionAndTypeManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                ImmutableList.of(leftValue, constant(100L, BIGINT)));

        // IMPORTANT: All left output variables must come before all right output variables
        JoinNode originalNode = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join_with_filter"),
                JoinType.INNER,
                leftSource,
                rightSource,
                ImmutableList.of(criteria),
                ImmutableList.of(leftKey, leftValue, rightKey),  // left vars first, then right vars
                Optional.of(filterExpression),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        // When: Clone the join node
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testCloneJoinNodeWithHashVariables()
    {
        // Given: JoinNode with hash variables for distributed execution
        VariableReferenceExpression leftKey = variable("left_key", BIGINT);
        VariableReferenceExpression rightKey = variable("right_key", BIGINT);
        VariableReferenceExpression leftHash = variable("left_hash", BIGINT);
        VariableReferenceExpression rightHash = variable("right_hash", BIGINT);

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = ImmutableMap.of(
                leftKey, new TestingColumnHandle("left_key"),
                leftHash, new TestingColumnHandle("left_hash"));

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = ImmutableMap.of(
                rightKey, new TestingColumnHandle("right_key"),
                rightHash, new TestingColumnHandle("right_hash"));

        TableScanNode leftSource = new TableScanNode(
                Optional.empty(),
                ID_ALLOCATOR.getNextId(),
                createTableHandle("left_table"),
                ImmutableList.of(leftKey, leftHash),
                leftAssignments,
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        TableScanNode rightSource = new TableScanNode(
                Optional.empty(),
                ID_ALLOCATOR.getNextId(),
                createTableHandle("right_table"),
                ImmutableList.of(rightKey, rightHash),
                rightAssignments,
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        EquiJoinClause criteria = new EquiJoinClause(leftKey, rightKey);

        JoinNode originalNode = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join_with_hash"),
                JoinType.INNER,
                leftSource,
                rightSource,
                ImmutableList.of(criteria),
                ImmutableList.of(leftKey, rightKey),
                Optional.empty(),
                Optional.of(leftHash),
                Optional.of(rightHash),
                Optional.empty(),
                ImmutableMap.of());

        // When: Clone the join node
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testCloneGroupReferenceWithLookup()
    {
        // Given: A GroupReference node with a lookup that can resolve it
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        TableScanNode resolvedPlan = createTableScan(orderkey);

        // Create a lookup using the static factory method
        Lookup lookup = Lookup.from(groupRef -> java.util.stream.Stream.of(resolvedPlan));

        GroupReference groupRef = new GroupReference(
                Optional.empty(),
                new PlanNodeId("group_ref_1"),
                0,
                ImmutableList.of(orderkey),
                Optional.empty());

        // When: Clone with lookup provided
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(
                groupRef,
                variableAllocator,
                ID_ALLOCATOR,
                lookup);

        // Then: Should resolve the GroupReference and clone the resolved plan
        TableScanNode clonedNode = (TableScanNode) cloned.getPlan();

        // Verify it's a cloned TableScanNode, not the original
        assertNotEquals(clonedNode.getId(), resolvedPlan.getId());

        // Verify variables were cloned
        assertEquals(clonedNode.getOutputVariables().size(), 1);
        assertEquals(clonedNode.getOutputVariables().get(0).getName(), "orderkey");
        assertNotSame(clonedNode.getOutputVariables().get(0), orderkey);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCloneGroupReferenceWithoutLookupFails()
    {
        // Given: A GroupReference node without a lookup
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);

        GroupReference groupRef = new GroupReference(
                Optional.empty(),
                new PlanNodeId("group_ref_2"),
                0,
                ImmutableList.of(orderkey),
                Optional.empty());

        // When: Try to clone without lookup (lookup = null)
        // Then: Should fail because GroupReference cannot be cloned without resolution
        PlanClonerWithVariableMapping.clonePlan(groupRef, variableAllocator, ID_ALLOCATOR, null);
    }

    @Test
    public void testCloneUnionNode()
    {
        // Given: UnionNode combining two table scans
        VariableReferenceExpression var1 = variable("var1", BIGINT);
        VariableReferenceExpression var2 = variable("var2", BIGINT);
        VariableReferenceExpression output = variable("output", BIGINT);

        TableScanNode source1 = createTableScan(var1);
        TableScanNode source2 = createTableScan(var2);

        UnionNode originalNode = new UnionNode(
                Optional.empty(),
                new PlanNodeId("union_1"),
                ImmutableList.of(source1, source2),
                ImmutableList.of(output),
                ImmutableMap.of(output, ImmutableList.of(var1, var2)));

        // When: Clone the UnionNode
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    @Test
    public void testCloneAggregationNode()
    {
        // Given: AggregationNode with grouping
        VariableReferenceExpression orderkey = variable("orderkey", BIGINT);
        TableScanNode source = createTableScan(orderkey);

        AggregationNode originalNode = new AggregationNode(
                Optional.empty(),
                new PlanNodeId("agg_1"),
                source,
                ImmutableMap.of(),
                new AggregationNode.GroupingSetDescriptor(
                        ImmutableList.of(orderkey),
                        1,
                        ImmutableSet.of(0)),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // When: Clone the AggregationNode
        PlanClonerWithVariableMapping.ClonedPlan cloned = PlanClonerWithVariableMapping.clonePlan(originalNode, variableAllocator, ID_ALLOCATOR);

        // Then: Verify plans are structurally identical except for variable names
        assertPlansIdenticalExceptVariables(originalNode, cloned.getPlan(), cloned.getVariableMapping());
    }

    // Helper methods

    /**
     * Asserts that two plans are structurally identical except for variable names.
     * Uses PlanPrinter to serialize both plans, then compares them after normalizing variable names
     * according to the provided mapping.
     */
    private void assertPlansIdenticalExceptVariables(
            PlanNode originalPlan,
            PlanNode clonedPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
    {
        // First verify variable naming follows the convention (baseName -> baseName, baseName_0, baseName_1, etc.)
        for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : variableMapping.entrySet()) {
            String originalName = entry.getKey().getName();
            String clonedName = entry.getValue().getName();

            // Extract base name from original (strip any existing _N suffix)
            String baseName = stripNumericSuffix(originalName);

            // Cloned name should be either baseName or baseName_N
            assertTrue(clonedName.equals(baseName) || clonedName.matches(baseName + "_\\d+"),
                    String.format("Cloned variable '%s' should follow naming convention from original '%s' (expected '%s' or '%s_N')",
                            clonedName, originalName, baseName, baseName));

            // Verify types match
            assertEquals(entry.getValue().getType(), entry.getKey().getType(),
                    "Variable types should be preserved");
        }

        // DEEP CONSISTENCY CHECKS: Use existing Presto validators to verify logical consistency
        Metadata metadata = createTestMetadataManager();

        // 1. Validate Dependencies: Ensures all variable references are valid and provided by source nodes
        try {
            new ValidateDependenciesChecker().validate(clonedPlan, session, metadata, WarningCollector.NOOP);
        }
        catch (RuntimeException e) {
            throw new AssertionError("Cloned plan failed dependency validation: " + e.getMessage(), e);
        }

        // 2. Type Validation: Ensures all types are consistent throughout the plan
        try {
            new TypeValidator().validate(clonedPlan, session, metadata, WarningCollector.NOOP);
        }
        catch (RuntimeException e) {
            throw new AssertionError("Cloned plan failed type validation: " + e.getMessage(), e);
        }

        // 3. Variable instance isolation: Ensure no variable instances are shared between original and cloned plans
        Set<VariableReferenceExpression> originalVars = VariablesExtractor.extractUnique(originalPlan);
        Set<VariableReferenceExpression> clonedVars = VariablesExtractor.extractUnique(clonedPlan);

        // Use identity-based set to check for shared instances (more efficient than O(n²) loop)
        Set<VariableReferenceExpression> originalVarIdentities = Collections.newSetFromMap(new IdentityHashMap<>());
        originalVarIdentities.addAll(originalVars);

        for (VariableReferenceExpression clonedVar : clonedVars) {
            assertTrue(!originalVarIdentities.contains(clonedVar),
                    String.format("Variable instance must not be shared between original and cloned plans: %s",
                            clonedVar.getName()));
        }

        // Serialize both plans to text and normalize variable names for comparison
        String normalizedOriginal = serializeAndNormalize(originalPlan, variableMapping, true);
        String normalizedCloned = serializeAndNormalize(clonedPlan, variableMapping, false);

        assertEquals(normalizedCloned, normalizedOriginal,
                "Plans should be structurally identical after normalizing variable names");
    }

    /**
     * Strips numeric suffix from variable name, following VariableAllocator's logic.
     * Examples: "orderkey_0" -> "orderkey", "var_123" -> "var", "orderkey" -> "orderkey"
     */
    private static String stripNumericSuffix(String name)
    {
        int index = name.lastIndexOf("_");
        if (index > 0) {
            String tail = name.substring(index + 1);
            // Only strip if tail is numeric
            if (tail.matches("\\d+")) {
                return name.substring(0, index);
            }
        }
        return name;
    }

    /**
     * Serializes a plan to text and normalizes all variable names.
     * For the original plan, replaces each variable with its base name.
     * For the cloned plan, uses the mapping to replace cloned variables with their original base names.
     */
    private String serializeAndNormalize(
            PlanNode plan,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping,
            boolean isOriginal)
    {
        // Build TypeProvider with ALL variables from the actual plan (not just the mapping)
        // This ensures PlanPrinter sees all variables and serializes them consistently
        Set<VariableReferenceExpression> planVariables = VariablesExtractor.extractUnique(plan);
        TypeProvider typeProvider = TypeProvider.fromVariables(planVariables);

        // Use PlanPrinter to serialize the plan
        String planText = PlanPrinter.textLogicalPlan(
                plan,
                typeProvider,
                StatsAndCosts.empty(),
                functionAndTypeManager,
                session,
                0);

        // Normalize variable names in the text
        if (isOriginal) {
            // For original: replace each variable name with its base name
            for (VariableReferenceExpression var : variableMapping.keySet()) {
                String originalName = var.getName();
                String baseName = stripNumericSuffix(originalName);
                // Replace variable references in the text
                planText = planText.replaceAll("\\b" + originalName + "\\b", baseName);
            }
        }
        else {
            // For cloned: replace each cloned variable with the base name of its original
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : variableMapping.entrySet()) {
                String originalName = entry.getKey().getName();
                String clonedName = entry.getValue().getName();
                String baseName = stripNumericSuffix(originalName);
                // Replace cloned variable references with base name
                planText = planText.replaceAll("\\b" + clonedName + "\\b", baseName);
            }
        }

        // Also normalize PlanNodeIds since they will differ
        // PlanNodeIds appear after "PlanNodeId " and before "]"
        // Examples: "PlanNodeId 123]", "PlanNodeId table_scan_1]", "PlanNodeId 10,20,30]"
        planText = planText.replaceAll("PlanNodeId [^\\]]+\\]", "PlanNodeId ID]");

        // Sort assignment lines to make output deterministic
        // Assignment lines follow the pattern: "            variable := ColumnHandle@..."
        String[] lines = planText.split("\n");
        StringBuilder result = new StringBuilder();
        List<String> assignmentBuffer = new ArrayList<>();

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            // Check if this is an assignment line (starts with whitespace, contains :=)
            if (line.trim().contains(":=") && line.startsWith("            ")) {
                assignmentBuffer.add(line);
                // Check if next line is also an assignment or if we're at the end
                if (i == lines.length - 1 || !lines[i + 1].startsWith("            ") || !lines[i + 1].trim().contains(":=")) {
                    // Sort and flush the buffer
                    assignmentBuffer.sort(String::compareTo);
                    for (String assignment : assignmentBuffer) {
                        result.append(assignment).append("\n");
                    }
                    assignmentBuffer.clear();
                }
            }
            else {
                result.append(line).append("\n");
            }
        }

        return result.toString().trim();
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
