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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.testng.Assert.assertTrue;

public class TestAssignments
{
    private final Assignments assignments = assignment(new VariableReferenceExpression("test", BIGINT), TRUE_LITERAL);

    @Test(expectedExceptions = {UnsupportedOperationException.class})
    public void testOutputsImmutable()
    {
        List<VariableReferenceExpression> outputs = assignments.getOutputs();
        // should throw as it is an unmodifiableList
        outputs.add(new VariableReferenceExpression("test", BIGINT));
    }

    @Test
    public void testOutputsMemoized()
    {
        assertTrue(assignments.getOutputs() == assignments.getOutputs());
    }
}
