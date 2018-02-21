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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern.Ordering;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_SORT;
import static com.facebook.presto.SystemSessionProperties.LOCAL_DISTRIBUTED_SORT;
import static com.facebook.presto.SystemSessionProperties.REDISTRIBUTE_SORT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.sql.tree.SortItem.Ordering.DESCENDING;

public class TestDistributedSort
        extends BasePlanTest
{
    @Test
    public void testSimpleDistributedSortNoRedistribution()
    {
        ImmutableList<Ordering> orderBy = ImmutableList.of(sort("ORDERKEY", ASCENDING, LAST));
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey", distributedSortNoRedistribution(), false,
                output(
                        exchange(REMOTE, GATHER, orderBy,
                                exchange(LOCAL, GATHER, orderBy,
                                        sort(orderBy,
                                                exchange(LOCAL, REPARTITION,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testSimpleDistributedSortWithRedistribution()
    {
        ImmutableList<Ordering> orderBy = ImmutableList.of(sort("ORDERKEY", DESCENDING, LAST));
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey DESC", distributedSortWithRedistribution(), false,
                output(
                        exchange(REMOTE, GATHER, orderBy,
                                exchange(LOCAL, GATHER, orderBy,
                                        sort(orderBy,
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testSimpleDistributedSortNoLocalDistributedSort()
    {
        ImmutableList<Ordering> orderBy = ImmutableList.of(sort("ORDERKEY", ASCENDING, LAST));
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey", distributedSortNoLocalDistributedSort(), false,
                output(
                        exchange(REMOTE, GATHER, orderBy,
                                sort(orderBy,
                                        exchange(LOCAL, GATHER,
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY", "orderkey"))))))));
    }

    private Session distributedSortNoRedistribution()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_SORT, Boolean.toString(true))
                .setSystemProperty(REDISTRIBUTE_SORT, Boolean.toString(false))
                .setSystemProperty(LOCAL_DISTRIBUTED_SORT, Boolean.toString(true))
                .build();
    }

    private Session distributedSortWithRedistribution()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_SORT, Boolean.toString(true))
                .setSystemProperty(REDISTRIBUTE_SORT, Boolean.toString(true))
                .setSystemProperty(LOCAL_DISTRIBUTED_SORT, Boolean.toString(true))
                .build();
    }

    private Session distributedSortNoLocalDistributedSort()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(DISTRIBUTED_SORT, Boolean.toString(true))
                .setSystemProperty(REDISTRIBUTE_SORT, Boolean.toString(true))
                .setSystemProperty(LOCAL_DISTRIBUTED_SORT, Boolean.toString(false))
                .build();
    }
}
