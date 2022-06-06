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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TestTableConstraintsConnectorFactory;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.USE_MARK_DISTINCT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static java.util.Collections.emptyList;

public class TestRedundantAggregateDistinctRemoval
        extends BaseRuleTest
{
    private LogicalPropertiesProviderImpl logicalPropertiesProvider;

    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of(USE_MARK_DISTINCT, Boolean.toString(false), "exploit_constraints", Boolean.toString(true)), Optional.of(1), new TestTableConstraintsConnectorFactory(1));
        logicalPropertiesProvider = new LogicalPropertiesProviderImpl(new FunctionResolution(tester.getMetadata().getFunctionAndTypeManager()));
    }

    @Test
    public void singleTableTests()
    {
        Map<String, ExpectedValueProvider<FunctionCall>> aggregations;
        aggregations = ImmutableMap.of(
                "count", functionCall("count", false, ImmutableList.of(anySymbol())),
                "avg", functionCall("avg", true, ImmutableList.of(anySymbol())));

        //linenumber, orderkey is a unique key, so count's distinct can be removed
        tester().assertThat(ImmutableSet.of(new RemoveRedundantAggregateDistinct()), logicalPropertiesProvider)
                .on("SELECT count(distinct linenumber), avg(distinct tax) FROM lineitem GROUP BY orderkey")
                .matches(output(
                        node(ProjectNode.class,
                                aggregation(aggregations,
                                        tableScan("lineitem")))));

        aggregations = ImmutableMap.of(
                "count", functionCall("count", false, ImmutableList.of(anySymbol())),
                "avg", functionCall("avg", false, ImmutableList.of(anySymbol())));

        //single row per group, so all distinct's are redundant
        tester().assertThat(ImmutableSet.of(new RemoveRedundantAggregateDistinct()), logicalPropertiesProvider)
                .on("SELECT count(distinct quantity), avg(distinct tax) FROM lineitem GROUP BY orderkey, linenumber")
                .matches(output(
                        node(ProjectNode.class,
                                aggregation(aggregations,
                                        tableScan("lineitem")))));

        aggregations = ImmutableMap.of(
                "count", functionCall("count", false, ImmutableList.of(anySymbol())),
                "avg", functionCall("avg", false, ImmutableList.of(anySymbol())));

        //single row input to aggregation, so all distinct's are redundant
        tester().assertThat(ImmutableSet.of(new RemoveRedundantAggregateDistinct()), logicalPropertiesProvider)
                .on("SELECT count(distinct quantity), avg(distinct tax) FROM lineitem where orderkey = 10 and linenumber=100 GROUP BY orderkey")
                .matches(output(
                        node(ProjectNode.class,
                                aggregation(aggregations,
                                        anyTree(tableScan("lineitem"))))));

        // Negative test cases
        aggregations = ImmutableMap.of(
                "count", functionCall("count", true, ImmutableList.of(anySymbol())),
                "avg", functionCall("avg", true, ImmutableList.of(anySymbol())));

        // Neither {quantity, orderkey} or {tax, orderkey} is a unique key. Note that the output of the aggregation is a single record but the input is not
        tester().assertThat(ImmutableSet.of(new RemoveRedundantAggregateDistinct()), logicalPropertiesProvider)
                .on("SELECT count(distinct quantity), avg(distinct tax) FROM lineitem where orderkey = 10 GROUP BY orderkey")
                .matches(output(
                        node(ProjectNode.class,
                                aggregation(aggregations,
                                        anyTree(tableScan("lineitem"))))));

        aggregations = ImmutableMap.of(
                "count", functionCall("count", false, ImmutableList.of(anySymbol())),
                "avg", functionCall("avg", true, ImmutableList.of(anySymbol())));

        //RemoveRedundantAggregateDistinct is not supplied
        tester().assertThat(ImmutableSet.of(), logicalPropertiesProvider)
                .on("SELECT count(distinct linenumber), avg(distinct tax) FROM lineitem GROUP BY orderkey")
                .doesNotMatch(output(
                        node(ProjectNode.class,
                                aggregation(aggregations,
                                        tableScan("lineitem")))));
    }

    @Test
    public void complexQueryTests()
    {
        Map<String, ExpectedValueProvider<FunctionCall>> aggregations = ImmutableMap.of("count", functionCall("count", false, ImmutableList.of(anySymbol())));
        tester().assertThat(ImmutableSet.of(new MergeLimitWithDistinct(), new RemoveRedundantAggregateDistinct()), logicalPropertiesProvider)
                .on("select count(distinct totalprice) from orders o inner join customer c on o.custkey = c.custkey group by orderkey")
                .matches(output(anyTree(
                        aggregation(
                                aggregations,
                                join(JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderkey", "orderkey", "custkey", "custkey")),
                                        tableScan("customer", ImmutableMap.of("custkey_0", "custkey")))))));
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
        RuleTester newTester = new RuleTester(emptyList(), ImmutableMap.of(USE_MARK_DISTINCT, Boolean.toString(false), "exploit_constraints", Boolean.toString(false)));

        Map<String, ExpectedValueProvider<FunctionCall>> aggregations;
        aggregations = ImmutableMap.of(
                "count", functionCall("count", true, ImmutableList.of(anySymbol())),
                "avg", functionCall("avg", true, ImmutableList.of(anySymbol())));

        //Rule is not applied, so the distinct should be true for both aggregations.
        newTester.assertThat(ImmutableSet.of(new RemoveRedundantAggregateDistinct()), logicalPropertiesProvider)
                .on("SELECT count(distinct linenumber), avg(distinct tax) FROM lineitem GROUP BY orderkey")
                .matches(output(
                        node(ProjectNode.class,
                                aggregation(aggregations,
                                        tableScan("lineitem")))));
    }
}
