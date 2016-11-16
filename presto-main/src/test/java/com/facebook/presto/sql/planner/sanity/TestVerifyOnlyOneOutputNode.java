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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestVerifyOnlyOneOutputNode
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testValidateSuccessful()
            throws Exception
    {
        // random seemingly valid plan
        PlanNode root =
                new OutputNode(idAllocator.getNextId(),
                        new ProjectNode(idAllocator.getNextId(),
                                new ValuesNode(
                                        idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of()
                                ),
                                ImmutableMap.of()
                        ), ImmutableList.of(), ImmutableList.of()
                );
        new VerifyOnlyOneOutputNode().validate(root, null, null, null, null);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testValidateFailed()
            throws Exception
    {
        // random plan with 2 output nodes
        PlanNode root =
                new OutputNode(idAllocator.getNextId(),
                        new ExplainAnalyzeNode(idAllocator.getNextId(),
                                new OutputNode(idAllocator.getNextId(),
                                        new ProjectNode(idAllocator.getNextId(),
                                                new ValuesNode(
                                                        idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of()
                                                ),
                                                ImmutableMap.of()
                                        ), ImmutableList.of(), ImmutableList.of()
                                ), new Symbol("a")
                        ),
                        ImmutableList.of(), ImmutableList.of()
                );
        new VerifyOnlyOneOutputNode().validate(root, null, null, null, null);
    }
}
