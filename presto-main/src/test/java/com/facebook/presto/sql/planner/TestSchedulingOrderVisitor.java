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

package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestSchedulingOrderVisitor
{
    @Test
    public void testJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata());
        TableScanNode a = planBuilder.tableScan(emptyList(), emptyMap());
        TableScanNode b = planBuilder.tableScan(emptyList(), emptyMap());
        List<PlanNodeId> order = scheduleOrder(planBuilder.join(JoinNode.Type.INNER, a, b));
        assertEquals(order, ImmutableList.of(b.getId(), a.getId()));
    }

    @Test
    public void testIndexJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata());
        TableScanNode a = planBuilder.tableScan(emptyList(), emptyMap());
        TableScanNode b = planBuilder.tableScan(emptyList(), emptyMap());
        List<PlanNodeId> order = scheduleOrder(planBuilder.indexJoin(IndexJoinNode.Type.INNER, a, b));
        assertEquals(order, ImmutableList.of(b.getId(), a.getId()));
    }

    @Test
    public void testSemiJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata());
        Symbol sourceJoin = planBuilder.symbol("sourceJoin");
        TableScanNode a = planBuilder.tableScan(ImmutableList.of(sourceJoin), ImmutableMap.of(sourceJoin, new TestingColumnHandle("sourceJoin")));
        Symbol filteringSource = planBuilder.symbol("filteringSource");
        TableScanNode b = planBuilder.tableScan(ImmutableList.of(filteringSource), ImmutableMap.of(filteringSource, new TestingColumnHandle("filteringSource")));
        List<PlanNodeId> order = scheduleOrder(planBuilder.semiJoin(
                sourceJoin,
                filteringSource,
                planBuilder.symbol("semiJoinOutput"),
                Optional.empty(),
                Optional.empty(),
                a,
                b));
        assertEquals(order, ImmutableList.of(b.getId(), a.getId()));
    }
}
