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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

public class TestPlanNodeDeepCopy
        extends BasePlanTest
{
    /// TODO Remove this test
    @Test
    public void test()
    {
        Plan queryPlan = plan("SELECT * FROM (VALUES 1, 2, 3) t(x)");


        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new PlanVariableAllocator(queryPlan.getTypes().allVariables());
        Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings = new HashMap<>();
        PlanNode rootNodeCopy = queryPlan.getRoot().deepCopy(planNodeIdAllocator, variableAllocator, variableMappings);
        assertNodesAreDeepCopied(queryPlan.getRoot(), rootNodeCopy, variableMappings);
    }

    @Test
    public void testValuesNode()
    {
        Plan queryPlan = plan("SELECT * FROM (VALUES 1, 2, 3) t(x)");


        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new PlanVariableAllocator(queryPlan.getTypes().allVariables());
        Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings = new HashMap<>();
        PlanNode rootNodeCopy = queryPlan.getRoot().deepCopy(planNodeIdAllocator, variableAllocator, variableMappings);
        assertNodesAreDeepCopied(queryPlan.getRoot(), rootNodeCopy, variableMappings);
    }

    private void assertNodesAreDeepCopied(PlanNode original, PlanNode copy,  Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings)
    {
        assertEquals(original.getClass(), copy.getClass(), String.format("'%s' type of the plan node does not match the type '%s' of original node", copy.getClass(), original.getClass()));
        assertNotSame(original.getId(), copy.getId());
        assertEquals(original.getSourceLocation(), copy.getSourceLocation());
        assertOutputVariablesDeepCopied(original, copy, variableMappings);
        if (copy instanceof OutputNode)
        {
            OutputNode originalOutputNode = (OutputNode) original;
            OutputNode copyOutputNode = (OutputNode) copy;
            assertNotSame(originalOutputNode, copyOutputNode, "Two nodes are pointing to the same heap location and thus not copied");
            assertNodesAreDeepCopied(originalOutputNode.getSource(), ((OutputNode) copy).getSource(), variableMappings);
            assertEquals(originalOutputNode.getColumnNames().size(), copyOutputNode.getColumnNames().size());
            for (int i = 0; i < originalOutputNode.getColumnNames().size(); ++i)
            {
                assertEquals(originalOutputNode.getColumnNames().get(i), copyOutputNode.getColumnNames().get(i));
            }
        }
        else if (copy instanceof ValuesNode)
        {
            ValuesNode originalOutputNode = (ValuesNode) original;
            ValuesNode copyOutputNode = (ValuesNode) copy;
            assertNotSame(originalOutputNode, copyOutputNode, "Two nodes are pointing to the same heap location and thus not copied");
            for (int row = 0; row < originalOutputNode.getRows().size(); ++row)
            {
                for (int col = 0; col < originalOutputNode.getRows().get(row).size(); ++col)
                {
                    assertNotSame(originalOutputNode.getRows().get(row).get(col), copyOutputNode.getRows().get(row).get(col));
                    assertEquals(originalOutputNode.getRows().get(row).get(col), copyOutputNode.getRows().get(row).get(col));
                }
            }
        }
    }

    private void assertOutputVariablesDeepCopied(PlanNode original, PlanNode copy, Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings)
    {
        assertEquals(original.getOutputVariables().size(), copy.getOutputVariables().size());
        for(int i = 0; i < original.getOutputVariables().size(); ++i)
        {
            assertNotSame(original.getOutputVariables().get(i), copy.getOutputVariables().get(i));
            assertEquals(variableMappings.get(original.getOutputVariables().get(i)), copy.getOutputVariables().get(i));
        }
    }
}
