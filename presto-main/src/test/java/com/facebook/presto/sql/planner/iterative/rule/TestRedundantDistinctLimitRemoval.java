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

import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.TestTableConstraintsConnectorFactory;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static java.util.Collections.emptyList;

public class TestRedundantDistinctLimitRemoval
        extends BaseRuleTest
{
    private LogicalPropertiesProviderImpl logicalPropertiesProvider;

    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of("exploit_constraints", Boolean.toString(true)), Optional.of(1), new TestTableConstraintsConnectorFactory(1));
        logicalPropertiesProvider = new LogicalPropertiesProviderImpl(new FunctionResolution(tester.getMetadata().getFunctionAndTypeManager()));
    }

    @Test
    public void singleTableTests()
    {
        tester().assertThat(new RemoveRedundantDistinctLimit(), logicalPropertiesProvider)
                .on(p ->
                        p.distinctLimit(
                                10,
                                ImmutableList.of(p.variable("c")),
                                p.values(1, p.variable("c"))))
                .matches(node(ValuesNode.class));

        tester().assertThat(new RemoveRedundantDistinctLimit(), logicalPropertiesProvider)
                .on(p ->
                        p.distinctLimit(
                                0,
                                ImmutableList.of(p.variable("c")),
                                p.values(1, p.variable("c"))))
                .matches(node(ValuesNode.class));

        tester().assertThat(ImmutableSet.of(new MergeLimitWithDistinct(), new RemoveRedundantDistinctLimit()), logicalPropertiesProvider)
                .on("SELECT distinct(c) FROM (SELECT count(*) as c FROM orders) LIMIT 10")
                .validates(plan -> assertNodeRemovedFromPlan(plan, DistinctLimitNode.class));

        //negative test
        tester().assertThat(ImmutableSet.of(new MergeLimitWithDistinct(), new RemoveRedundantDistinctLimit()), logicalPropertiesProvider)
                .on("SELECT distinct(c) FROM (SELECT count(*) as c FROM orders GROUP BY orderkey) LIMIT 10")
                .matches(output(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        tableScan("orders")))));

        //TODO where are the constraints use cases?!
    }

    @Test
    public void complexQueryTests()
    {
        //TODO more join, complex query tests
        tester().assertThat(ImmutableSet.of(new MergeLimitWithDistinct(), new RemoveRedundantDistinctLimit()), logicalPropertiesProvider)
                .on("select distinct totalprice from orders o inner join customer c on o.custkey = c.custkey where o.orderkey=10 limit 2")
                .validates(plan -> assertNodeRemovedFromPlan(plan, DistinctLimitNode.class));

        //negative tests
        tester().assertThat(ImmutableSet.of(new MergeLimitWithDistinct(), new RemoveRedundantDistinctLimit()), logicalPropertiesProvider)
                .on("select distinct totalprice from orders o inner join customer c on o.custkey = c.custkey where o.orderkey>10 limit 2")
                .validates(plan -> assertNodePresentInPlan(plan, DistinctLimitNode.class));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new RemoveRedundantDistinctLimit(), logicalPropertiesProvider)
                .on(p ->
                        p.distinctLimit(
                                10,
                                ImmutableList.of(p.variable("c")),
                                p.values(10, p.variable("c"))))
                .doesNotFire();
    }

    @Test
    public void testFeatureDisabled()
    {
        // Disable the feature and verify that optimization rule is not applied.
        RuleTester newTester = new RuleTester(emptyList(), ImmutableMap.of("exploit_constraints", Boolean.toString(false)));

        newTester.assertThat(ImmutableSet.of(new MergeLimitWithDistinct(), new RemoveRedundantDistinctLimit()), logicalPropertiesProvider)
                .on("select distinct totalprice from orders o inner join customer c on o.custkey = c.custkey where o.orderkey=10 limit 2")
                .validates(plan -> assertNodePresentInPlan(plan, DistinctLimitNode.class));
    }
}
