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
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
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

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.util.Collections.emptyList;

public class TestRedundantLimitRemoval
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
        tester().assertThat(new RemoveRedundantLimit(), logicalPropertiesProvider)
                .on(p ->
                        p.limit(
                                10,
                                p.aggregation(builder -> builder
                                        .addAggregation(p.variable("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .globalGrouping()
                                        .source(p.values(p.variable("foo"))))))
                .matches(
                        node(AggregationNode.class,
                                node(ValuesNode.class)));

        //remove limit with ties
        tester().assertThat(new RemoveRedundantLimit(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression c = p.variable("c");
                    return p.limit(
                            10,
                            p.values(5, c));
                })
                .matches(values("c"));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantLimit()), logicalPropertiesProvider)
                .on("SELECT count(*) FROM orders LIMIT 2")
                .validates(plan -> assertNodeRemovedFromPlan(plan, LimitNode.class));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantLimit()), logicalPropertiesProvider)
                .on("SELECT * FROM (VALUES 1,2,3,4,5,6) AS t1 LIMIT 10")
                .matches(output(
                        values(ImmutableList.of("x"))));

        tester().assertThat(ImmutableSet.of(new RemoveRedundantLimit()), logicalPropertiesProvider)
                .on("select a from orders inner join (values(2)) t(a) ON orderkey=1 limit 3")
                .validates(plan -> assertNodeRemovedFromPlan(plan, LimitNode.class));

        //negative tests
        tester().assertThat(ImmutableSet.of(new RemoveRedundantLimit()), logicalPropertiesProvider)
                .on("SELECT orderkey, count(*) FROM orders GROUP BY orderkey ORDER BY 1 LIMIT 10")
                .validates(plan -> assertNodePresentInPlan(plan, LimitNode.class));
        tester().assertThat(ImmutableSet.of(new RemoveRedundantLimit()), logicalPropertiesProvider)
                .on("select a from orders left join (values(2)) t(a) ON orderkey=1 limit 3")
                .validates(plan -> assertNodePresentInPlan(plan, LimitNode.class));
    }

    @Test
    public void complexQueryTests()
    {
        //TODO more join, complex query tests
        tester().assertThat(ImmutableSet.of(new RemoveRedundantLimit()), logicalPropertiesProvider)
                .on("select totalprice from orders o inner join customer c on o.custkey = c.custkey where o.orderkey=10 limit 2")
                .validates(plan -> assertNodeRemovedFromPlan(plan, LimitNode.class));

        //negative tests
        tester().assertThat(ImmutableSet.of(new RemoveRedundantLimit()), logicalPropertiesProvider)
                .on("select totalprice from orders o inner join customer c on o.custkey = c.custkey where o.orderkey>10 limit 2")
                .validates(plan -> assertNodePresentInPlan(plan, LimitNode.class));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new RemoveRedundantLimit(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression c = p.variable("c");
                    return p.limit(
                            10,
                            p.values(12, c));
                })
                .doesNotFire();
    }

    @Test
    public void testFeatureDisabled()
    {
        // Disable the feature and verify that optimization rule is not applied.
        RuleTester newTester = new RuleTester(emptyList(), ImmutableMap.of("exploit_constraints", Boolean.toString(false)));

        newTester.assertThat(ImmutableSet.of(new RemoveRedundantLimit()), logicalPropertiesProvider)
            .on("select totalprice from orders o inner join customer c on o.custkey = c.custkey where o.orderkey=10 limit 2")
            .validates(plan -> assertNodePresentInPlan(plan, LimitNode.class));
    }
}
