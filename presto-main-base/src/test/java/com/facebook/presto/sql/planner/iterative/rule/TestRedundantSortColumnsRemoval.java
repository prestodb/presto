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

import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topN;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.sql.tree.SortItem.Ordering.DESCENDING;
import static java.util.Collections.emptyList;

public class TestRedundantSortColumnsRemoval
        extends BaseRuleTest
{
    private LogicalPropertiesProviderImpl logicalPropertiesProvider;

    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of("exploit_constraints", Boolean.toString(true)));
        logicalPropertiesProvider = new LogicalPropertiesProviderImpl(new FunctionResolution(tester.getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver()));
    }

    @Test
    public void testRemoveRedundantColumnsFromTopN()
    {
        // OrderBy prefix matches GroupBy columns clause exactly
        tester().assertThat(ImmutableSet.of(new MergeLimitWithSort(), new RemoveRedundantTopNColumns()), logicalPropertiesProvider)
                .on("SELECT orderkey, custkey, sum(totalprice), min(orderdate) FROM orders GROUP BY orderkey, custkey " +
                        "ORDER BY orderkey, custkey, sum(totalprice), min(orderdate) LIMIT 10")
                .matches(topNMatchWith(ImmutableList.of(sort("ORDERKEY", ASCENDING, LAST), sort("CUSTKEY", ASCENDING, LAST))));

        // Flipped order matches too since the Grouping set remains the same
        tester().assertThat(ImmutableSet.of(new MergeLimitWithSort(), new RemoveRedundantTopNColumns()), logicalPropertiesProvider)
                .on("SELECT orderkey, custkey, sum(totalprice), min(orderdate) FROM orders GROUP BY orderkey, custkey " +
                        "ORDER BY custkey, orderkey, sum(totalprice), min(orderdate) LIMIT 10")
                .matches(topNMatchWith(ImmutableList.of(sort("CUSTKEY", ASCENDING, LAST), sort("ORDERKEY", ASCENDING, LAST))));

        // No impact due to sort direction
        tester().assertThat(ImmutableSet.of(new MergeLimitWithSort(), new RemoveRedundantTopNColumns()), logicalPropertiesProvider)
                .on("SELECT orderkey, custkey, sum(totalprice), min(orderdate) FROM orders GROUP BY orderkey, custkey " +
                        "ORDER BY custkey DESC, orderkey ASC, sum(totalprice), min(orderdate) LIMIT 10")
                .matches(topNMatchWith(ImmutableList.of(sort("CUSTKEY", DESCENDING, LAST), sort("ORDERKEY", ASCENDING, LAST))));

        // Negative test - No prefix matches the grouping set, so TopN columns are not pruned
        tester().assertThat(ImmutableSet.of(new MergeLimitWithSort(), new RemoveRedundantTopNColumns()), logicalPropertiesProvider)
                .on("SELECT orderkey, custkey, sum(totalprice), min(orderdate) FROM orders GROUP BY orderkey, custkey " +
                        "ORDER BY orderkey, sum(totalprice), custkey, min(orderdate) LIMIT 10")
                .doesNotMatch(topNMatchWith(ImmutableList.of(sort("ORDERKEY", ASCENDING, LAST), sort("CUSTKEY", ASCENDING, LAST))));
    }

    @Test
    public void testRemoveRedundantColumnsFromSort()
    {
        // OrderBy prefix matches GroupBy columns clause exactly
        tester().assertThat(ImmutableSet.of(new RemoveRedundantSortColumns()), logicalPropertiesProvider)
                .on("SELECT orderkey, custkey, sum(totalprice), min(orderdate) FROM orders GROUP BY orderkey, custkey " +
                        "ORDER BY orderkey, custkey, sum(totalprice), min(orderdate)")
                .matches(sortWith(ImmutableList.of(sort("ORDERKEY", ASCENDING, LAST), sort("CUSTKEY", ASCENDING, LAST))));

        // Flipped order matches too since the Grouping set remains the same
        tester().assertThat(ImmutableSet.of(new RemoveRedundantSortColumns()), logicalPropertiesProvider)
                .on("SELECT orderkey, custkey, sum(totalprice), min(orderdate) FROM orders GROUP BY orderkey, custkey " +
                        "ORDER BY custkey, orderkey, sum(totalprice), min(orderdate)")
                .matches(sortWith(ImmutableList.of(sort("CUSTKEY", ASCENDING, LAST), sort("ORDERKEY", ASCENDING, LAST))));

        // No impact due to sort direction
        tester().assertThat(ImmutableSet.of(new RemoveRedundantSortColumns()), logicalPropertiesProvider)
                .on("SELECT orderkey, custkey, sum(totalprice), min(orderdate) FROM orders GROUP BY orderkey, custkey " +
                        "ORDER BY custkey DESC, orderkey ASC, sum(totalprice), min(orderdate)")
                .matches(sortWith(ImmutableList.of(sort("CUSTKEY", DESCENDING, LAST), sort("ORDERKEY", ASCENDING, LAST))));

        // Negative test - No prefix matches the grouping set, so TopN columns are not pruned
        tester().assertThat(ImmutableSet.of(new RemoveRedundantSortColumns()), logicalPropertiesProvider)
                .on("SELECT orderkey, custkey, sum(totalprice), min(orderdate) FROM orders GROUP BY orderkey, custkey " +
                        "ORDER BY orderkey, sum(totalprice), custkey, min(orderdate)")
                .doesNotMatch(sortWith(ImmutableList.of(sort("ORDERKEY", ASCENDING, LAST), sort("CUSTKEY", ASCENDING, LAST))));
    }

    private static PlanMatchPattern topNMatchWith(ImmutableList<PlanMatchPattern.Ordering> orderBy)
    {
        return output(
                topN(10, orderBy,
                        anyTree(
                                tableScan("orders", ImmutableMap.of(
                                        "ORDERKEY", "orderkey",
                                        "CUSTKEY", "custkey")))));
    }

    private static PlanMatchPattern sortWith(ImmutableList<PlanMatchPattern.Ordering> orderBy)
    {
        return output(
                sort(orderBy,
                        anyTree(
                                tableScan("orders", ImmutableMap.of(
                                        "ORDERKEY", "orderkey",
                                        "CUSTKEY", "custkey")))));
    }
}
