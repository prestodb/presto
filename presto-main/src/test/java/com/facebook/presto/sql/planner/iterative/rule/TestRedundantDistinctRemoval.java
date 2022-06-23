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
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TestTableConstraintsConnectorFactory;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Collections.emptyList;

public class TestRedundantDistinctRemoval
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
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT acctbal FROM customer WHERE custkey = 100")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT orderkey, custkey FROM orders")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT * FROM (VALUES (1,2)) t1 (c1, c2)")
                .matches(any(node(ValuesNode.class)));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT orderkey, custkey FROM orders")
                .matches(any(node(TableScanNode.class)));

        tester().assertThat(new RemoveRedundantDistinct(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression unique = p.variable("unique");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(c, unique)
                            .source(p.assignUniqueId(unique,
                                    p.values(5, c))));
                })
                .matches(assignUniqueId("unique",
                        node(ValuesNode.class)));

        // Negative test cases
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT custkey FROM orders")
                .matches(any(node(AggregationNode.class, (node(TableScanNode.class)))));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT custkey+5 FROM customer")
                .validates(plan -> assertNodePresentInPlan(plan, AggregationNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT acctbal FROM customer WHERE custkey + 5= 100")
                .validates(plan -> assertNodePresentInPlan(plan, AggregationNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT totalprice from orders where Orderkey in (1,2,3,3)")
                .validates(plan -> assertNodePresentInPlan(plan, AggregationNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT totalprice FROM orders WHERE orderkey = (SELECT orderkey from lineitem limit 1)")
                .validates(plan -> assertNodePresentInPlan(plan, AggregationNode.class));
    }

    //TODO add some outer join tests, including negative tests where keys should not propagate

    @Test
    public void testRedundantDistinctRemovalOneToNJoin()
    {
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("select distinct * from orders o inner join customer c on o.custkey = c.custkey")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("select distinct l.orderkey from orders o inner join customer c on o.custkey = c.custkey inner join lineitem l on o.orderkey = l.orderkey where o.orderkey = 10 and l.linenumber = 10")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("select distinct o.orderkey from orders o inner join customer c on o.custkey = c.custkey inner join lineitem l on o.orderkey = l.orderkey where o.orderkey = 10 and l.linenumber = 10")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("select distinct c.custkey from orders o inner join customer c on o.custkey = c.custkey inner join lineitem l on o.orderkey = l.orderkey where o.orderkey = 10 and l.linenumber = 10")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT distinct orderkey FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber in (1,2,3,4))")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
    }

    @Test
    public void testRedundantDistinctRemovalNtoMJoin()
    {
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("select distinct o.orderkey, c.custkey from orders o inner join customer c on o.custkey > c.custkey")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
    }

    @Test
    public void testRedundantDistinctRemovalOneToOneJoin()
    {
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("select distinct o.orderkey, l.linenumber from orders o inner join lineitem l on o.orderkey = l.orderkey where l.linenumber = 10")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("select distinct l.orderkey, l.linenumber from orders o inner join lineitem l on o.orderkey = l.orderkey where l.linenumber = 10")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
        tester().assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("SELECT DISTINCT o.orderkey FROM orders o LEFT JOIN customer c ON o.custkey = c.custkey")
                .validates(plan -> assertNodeRemovedFromPlan(plan, AggregationNode.class));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new RemoveRedundantDistinct(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression c = p.variable("c");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(c)
                            .source(p.limit(
                                    5,
                                    p.values(5, c))));
                })
                .doesNotFire();
    }

    @Test
    public void testFeatureDisabled()
    {
        // Disable the feature and verify that optimization rule is not applied.
        RuleTester newTester = new RuleTester(emptyList(), ImmutableMap.of("exploit_constraints", Boolean.toString(false)));

        newTester.assertThat(ImmutableSet.of(new RemoveRedundantDistinct()), logicalPropertiesProvider)
                .on("select distinct o.orderkey, l.linenumber from orders o inner join lineitem l on o.orderkey = l.orderkey where l.linenumber = 10")
                .validates(plan -> assertNodePresentInPlan(plan, AggregationNode.class));
    }
}
