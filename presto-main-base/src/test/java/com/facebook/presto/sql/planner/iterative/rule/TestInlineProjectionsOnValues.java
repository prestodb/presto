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

import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.INLINE_PROJECTIONS_ON_VALUES;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.Expressions.call;

public class TestInlineProjectionsOnValues
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOn()
    {
        tester().assertThat(new InlineProjectionsOnValues(tester.getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(INLINE_PROJECTIONS_ON_VALUES, "true")
                .on(p -> p.project(p.project(p.values(p.getIdAllocator().getNextId(), p.variable("a")),
                                assignment(p.variable("c"), p.variable("a"))),
                        assignment(p.variable("d"), p.variable("c"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnWithNonDeterministicFunction()
    {
        tester().assertThat(new InlineProjectionsOnValues(tester.getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(INLINE_PROJECTIONS_ON_VALUES, "true")
                .on(p -> p.project(p.values(p.getIdAllocator().getNextId(), p.variable("a")),
                        assignment(p.variable("b"), call(tester.getMetadata().getFunctionAndTypeManager(), "random", BIGINT, p.variable("a")))))
                .doesNotFire();
    }

    @Test
    public void testFireOnProjectFollowedByValues()
    {
        tester().assertThat(new InlineProjectionsOnValues(tester.getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(INLINE_PROJECTIONS_ON_VALUES, "true")
                //  Form the input planNode: ProjectNode -> ValuesNode
                .on(p -> p.project(p.values(p.getIdAllocator().getNextId(), p.variable("a")),
                        assignment(p.variable("c"), p.variable("a"))))
                // Ensure the PlanNode is optimized to a ValuesNode
                .matches(node(ValuesNode.class));
    }
}
