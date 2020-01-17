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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestSchedulingOrderVisitor
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        TableScanNode a = planBuilder.tableScan(emptyList(), emptyMap());
        TableScanNode b = planBuilder.tableScan(emptyList(), emptyMap());
        List<PlanNodeId> order = scheduleOrder(planBuilder.join(JoinNode.Type.INNER, a, b));
        assertEquals(order, ImmutableList.of(b.getId(), a.getId()));
    }

    @Test
    public void testIndexJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        TableScanNode a = planBuilder.tableScan(emptyList(), emptyMap());
        TableScanNode b = planBuilder.tableScan(emptyList(), emptyMap());
        List<PlanNodeId> order = scheduleOrder(planBuilder.indexJoin(IndexJoinNode.Type.INNER, a, b));
        assertEquals(order, ImmutableList.of(b.getId(), a.getId()));
    }

    @Test
    public void testSemiJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        VariableReferenceExpression sourceJoin = planBuilder.variable("sourceJoin");
        TableScanNode a = planBuilder.tableScan(ImmutableList.of(sourceJoin), ImmutableMap.of(sourceJoin, new TestingColumnHandle("sourceJoin")));
        VariableReferenceExpression filteringSource = planBuilder.variable("filteringSource");
        TableScanNode b = planBuilder.tableScan(ImmutableList.of(filteringSource), ImmutableMap.of(filteringSource, new TestingColumnHandle("filteringSource")));
        List<PlanNodeId> order = scheduleOrder(planBuilder.semiJoin(
                sourceJoin,
                filteringSource,
                planBuilder.variable("semiJoinOutput"),
                Optional.empty(),
                Optional.empty(),
                a,
                b));
        assertEquals(order, ImmutableList.of(b.getId(), a.getId()));
    }
}
