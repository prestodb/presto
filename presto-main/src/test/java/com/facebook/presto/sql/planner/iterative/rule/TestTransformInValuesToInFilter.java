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

import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.TRANSFORM_IN_VALUES_TO_IN_FILTER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestTransformInValuesToInFilter
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnWithTwoColumns()
    {
        tester().assertThat(new TransformInValuesToInFilter())
                .setSystemProperty(TRANSFORM_IN_VALUES_TO_IN_FILTER, "true")
                .on(p -> p.semiJoin(p.variable("x"),
                        p.variable("c"),
                        p.variable("d"),
                        Optional.empty(),
                        Optional.empty(),
                        p.values(p.variable("x"), p.variable("y")),
                        p.project(p.values(p.getIdAllocator().getNextId(), 0, p.variable("a"), p.variable("b")), assignment(p.variable("c"), p.variable("a")))))
                .doesNotFire();
    }

    @Test
    public void testFiresOnOneColumn()
    {
        tester().assertThat(new TransformInValuesToInFilter())
                .setSystemProperty(TRANSFORM_IN_VALUES_TO_IN_FILTER, "true")
                .on(p -> p.semiJoin(p.variable("x"),
                        p.variable("c"),
                        p.variable("d"),
                        Optional.empty(),
                        Optional.empty(),
                        p.values(p.variable("x"), p.variable("y")),
                        p.values(p.getIdAllocator().getNextId(), 2, p.variable("c"))))
                .matches(node(ProjectNode.class, values("x", "y")));
    }
}
