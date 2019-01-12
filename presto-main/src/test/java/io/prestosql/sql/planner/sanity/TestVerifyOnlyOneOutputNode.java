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
package io.prestosql.sql.planner.sanity;

import com.google.common.collect.ImmutableList;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

public class TestVerifyOnlyOneOutputNode
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testValidateSuccessful()
    {
        // random seemingly valid plan
        PlanNode root =
                new OutputNode(idAllocator.getNextId(),
                        new ProjectNode(idAllocator.getNextId(),
                                new ValuesNode(
                                        idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of()),
                                Assignments.of()
                        ), ImmutableList.of(), ImmutableList.of());
        new VerifyOnlyOneOutputNode().validate(root, null, null, null, null, WarningCollector.NOOP);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testValidateFailed()
    {
        // random plan with 2 output nodes
        PlanNode root =
                new OutputNode(idAllocator.getNextId(),
                        new ExplainAnalyzeNode(idAllocator.getNextId(),
                                new OutputNode(idAllocator.getNextId(),
                                        new ProjectNode(idAllocator.getNextId(),
                                                new ValuesNode(
                                                        idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of()),
                                                Assignments.of()
                                        ), ImmutableList.of(), ImmutableList.of()
                                ), new Symbol("a"),
                                false),
                        ImmutableList.of(), ImmutableList.of());
        new VerifyOnlyOneOutputNode().validate(root, null, null, null, null, WarningCollector.NOOP);
    }
}
