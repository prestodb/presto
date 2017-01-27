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

import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.emptyMap;

public class TestPruneValuesColumns
{
    private final RuleTester tester = new RuleTester();

    @Test
    public void test()
            throws Exception
    {
        tester.assertThat(new PruneValuesColumns())
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.values(p.symbol("foo", BIGINT))))
                .matches(
                        node(ProjectNode.class,
                                values(emptyMap())));
    }

    @Test
    public void testDoesNotFire()
            throws Exception
    {
        tester.assertThat(new PushLimitThroughMarkDistinct())
                .on(p ->
                        p.project(
                                Assignments.of(),
                                p.values()))
                .doesNotFire();
    }
}
