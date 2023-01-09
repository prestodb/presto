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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.TestTableConstraintsConnectorFactory;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.util.Collections.emptyList;

public class TestRedundantSortRemoval
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
        //sorting one record result of a single group aggregate
        tester().assertThat(new RemoveRedundantSort(), logicalPropertiesProvider)
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.variable("c")),
                                p.aggregation(builder -> builder
                                        .addAggregation(p.variable("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .globalGrouping()
                                        .source(p.values(p.variable("foo"))))))
                .matches(
                        node(AggregationNode.class,
                                node(ValuesNode.class)));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantSort()), logicalPropertiesProvider)
                .on("select max(totalprice) from orders order by 1")
                .validates(plan -> assertNodeRemovedFromPlan(plan, SortNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantSort()), logicalPropertiesProvider)
                .on("select count(*), totalprice from orders where totalprice = 101.10 group by totalprice order by 1, 2")
                .validates(plan -> assertNodeRemovedFromPlan(plan, SortNode.class));

        //zero cardinality
        tester().assertThat(new RemoveRedundantSort(), logicalPropertiesProvider)
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.variable("c")),
                                p.values(p.variable("foo"))))
                .matches(node(ValuesNode.class));

        //binding a primary key
        tester().assertThat(ImmutableSet.of(new RemoveRedundantSort()), logicalPropertiesProvider)
                .on("SELECT totalprice FROM orders WHERE orderkey = 10 ORDER BY totalprice")
                .validates(plan -> assertNodeRemovedFromPlan(plan, SortNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantSort()), logicalPropertiesProvider)
                .on("SELECT quantity FROM lineitem WHERE orderkey = 10 and linenumber = 100 ORDER BY quantity")
                .validates(plan -> assertNodeRemovedFromPlan(plan, SortNode.class));

        // Negative test cases
        //TODO add more negative tests, i.e. operators that do not propagate keys like
        tester().assertThat(ImmutableSet.of(new RemoveRedundantSort()), logicalPropertiesProvider)
                .on("SELECT quantity FROM lineitem WHERE orderkey = 10 ORDER BY quantity")
                .validates(plan -> assertNodePresentInPlan(plan, SortNode.class));

        tester().assertThat(ImmutableSet.of(), logicalPropertiesProvider)
                .on("select max(totalprice) from orders order by 1")
                .validates(plan -> assertNodePresentInPlan(plan, SortNode.class));
    }

    @Test
    public void complexQueryTests()
    {
        //TODO more join, complex query tests
        tester().assertThat(ImmutableSet.of(new RemoveRedundantSort()), logicalPropertiesProvider)
                .on("select totalprice from orders o inner join customer c on o.custkey = c.custkey where o.orderkey=10 order by totalprice")
                .validates(plan -> assertNodeRemovedFromPlan(plan, SortNode.class));

        //negative tests
        tester().assertThat(ImmutableSet.of(new RemoveRedundantSort()), logicalPropertiesProvider)
                .on("select totalprice from orders o inner join customer c on o.custkey = c.custkey where o.orderkey>10 order by totalprice")
                .validates(plan -> assertNodePresentInPlan(plan, SortNode.class));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new RemoveRedundantSort(), logicalPropertiesProvider)
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.variable("c")),
                                p.aggregation(builder -> builder
                                        .addAggregation(p.variable("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .singleGroupingSet(p.variable("foo"))
                                        .source(p.values(20, p.variable("foo"))))))
                .doesNotFire();
    }

    @Test
    public void testFeatureDisabled()
    {
        // Disable the feature and verify that optimization rule is not applied.
        RuleTester newTester = new RuleTester(emptyList(), ImmutableMap.of("exploit_constraints", Boolean.toString(false)));

        newTester.assertThat(ImmutableSet.of(new RemoveRedundantSort()), logicalPropertiesProvider)
                .on("select max(totalprice) from orders order by 1")
                .validates(plan -> assertNodePresentInPlan(plan, SortNode.class));
    }
}
