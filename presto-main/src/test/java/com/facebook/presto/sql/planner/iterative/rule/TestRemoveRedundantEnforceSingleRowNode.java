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
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;

public class TestRemoveRedundantEnforceSingleRowNode
        extends BaseRuleTest
{
    @Test
    public void testValidateEnforceSingleRowNodeRemoved()
    {
        tester().assertThat(new RemoveRedundantEnforceSingleRowNode())
                .on(p ->
                        p.enforceSingleRow(
                                p.values(1, p.variable("c"))))
                .matches(node(ValuesNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantEnforceSingleRowNode()))
                .on("SELECT * FROM orders WHERE orderkey = (SELECT min(orderkey) FROM lineitem)")
                .validates(plan -> assertNodeRemovedFromPlan(plan, EnforceSingleRowNode.class));

        // Remove multiple EnforceSingleRowNode's
        tester().assertThat(ImmutableSet.of(new RemoveRedundantEnforceSingleRowNode()))
                .on("SELECT * FROM orders WHERE orderkey = (SELECT min(orderkey) FROM lineitem) or orderkey = (SELECT max(orderkey) from lineitem)")
                .validates(plan -> assertNodeRemovedFromPlan(plan, EnforceSingleRowNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantEnforceSingleRowNode()))
                .on("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey = (SELECT 1) AND o2.orderkey = (SELECT 1) AND o1.orderkey + o2.orderkey = (SELECT 2)")
                .validates(plan -> assertNodeRemovedFromPlan(plan, EnforceSingleRowNode.class));

        // negative test
        tester().assertThat(ImmutableSet.of(new RemoveRedundantEnforceSingleRowNode()))
                .on("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 2)")
                .validates(plan -> assertNodePresentInPlan(plan, EnforceSingleRowNode.class));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveRedundantEnforceSingleRowNode())
                .on(p ->
                        p.enforceSingleRow(
                                p.values(10, p.variable("c"))))
                .doesNotFire();
    }
}
