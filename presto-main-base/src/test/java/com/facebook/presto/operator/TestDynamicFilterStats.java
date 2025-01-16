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
package com.facebook.presto.operator;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterStats
{
    private void assertDynamicFilterStatsEquals(DynamicFilterStats d1, DynamicFilterStats d2)
    {
        assertEquals(d1.empty(), d2.empty());
        assertEquals(d1.getProducerNodeIds(), d2.getProducerNodeIds());
    }

    @Test
    public void testCtor()
    {
        final PlanNodeId[] planNodes = new PlanNodeId[] {new PlanNodeId("1"), new PlanNodeId("2")};
        Set<PlanNodeId> planNodeSet = new HashSet<>(Arrays.asList(planNodes));
        DynamicFilterStats nonEmptyStats = new DynamicFilterStats(planNodeSet);
        assertFalse(nonEmptyStats.empty());
        assertEquals(nonEmptyStats.getProducerNodeIds(), planNodeSet);
        assertDynamicFilterStatsEquals(nonEmptyStats, nonEmptyStats);

        final PlanNodeId[] emptyPlanNodes = new PlanNodeId[] {};
        Set<PlanNodeId> emptyPlanNodeSet = new HashSet<>(Arrays.asList(emptyPlanNodes));
        DynamicFilterStats emptyStats = new DynamicFilterStats(emptyPlanNodeSet);
        assertTrue(emptyStats.empty());
        assertTrue(emptyStats.getProducerNodeIds().isEmpty());
        assertDynamicFilterStatsEquals(emptyStats, emptyStats);
    }

    @Test
    public void testCopy()
    {
        final PlanNodeId[] planNodes = new PlanNodeId[] {new PlanNodeId("1"), new PlanNodeId("2")};
        Set<PlanNodeId> planNodeSet = new HashSet<>(Arrays.asList(planNodes));
        DynamicFilterStats stats = new DynamicFilterStats(planNodeSet);

        DynamicFilterStats statsCopy = DynamicFilterStats.copyOf(stats);
        assertDynamicFilterStatsEquals(stats, statsCopy);
        assertFalse(statsCopy.empty());
        assertEquals(statsCopy.getProducerNodeIds(), planNodeSet);

        final PlanNodeId[] emptyPlanNodes = new PlanNodeId[] {};
        Set<PlanNodeId> emptyPlanNodeSet = new HashSet<>(Arrays.asList(emptyPlanNodes));
        DynamicFilterStats emptyStats = new DynamicFilterStats(emptyPlanNodeSet);
        DynamicFilterStats emptyStatsCopy = DynamicFilterStats.copyOf(emptyStats);
        assertDynamicFilterStatsEquals(emptyStats, emptyStatsCopy);
        assertTrue(emptyStatsCopy.empty());
        assertTrue(emptyStatsCopy.getProducerNodeIds().isEmpty());
    }

    @Test
    public void testMergeWith()
    {
        final PlanNodeId[] planNodes1 = new PlanNodeId[] {new PlanNodeId("1"), new PlanNodeId("2")};
        Set<PlanNodeId> planNodeSet1 = new HashSet<>(Arrays.asList(planNodes1));
        DynamicFilterStats stats1 = new DynamicFilterStats(planNodeSet1);
        assertEquals(stats1.getProducerNodeIds(), planNodeSet1);

        final PlanNodeId[] planNodes2 = new PlanNodeId[] {new PlanNodeId("2"), new PlanNodeId("3")};
        Set<PlanNodeId> planNodeSet2 = new
                HashSet<>(Arrays.asList(planNodes2));
        DynamicFilterStats stats2 = new DynamicFilterStats(planNodeSet2);
        assertEquals(stats2.getProducerNodeIds(), planNodeSet2);

        stats2.mergeWith(stats1);
        assertEquals(stats1.getProducerNodeIds(), planNodeSet1);
        final Set<PlanNodeId> expectedMergeNodeSet = ImmutableSet.of(new PlanNodeId("1"), new PlanNodeId("2"), new PlanNodeId("3"));
        assertEquals(stats2.getProducerNodeIds(), expectedMergeNodeSet);
        assertFalse(stats2.empty());

        final PlanNodeId[] emptyPlanNodes = new PlanNodeId[] {};
        Set<PlanNodeId> emptyPlanNodeSet = new HashSet<>(Arrays.asList(emptyPlanNodes));
        DynamicFilterStats emptyStats1 = new DynamicFilterStats(emptyPlanNodeSet);
        DynamicFilterStats emptyStats2 = new DynamicFilterStats(emptyPlanNodeSet);
        emptyStats2.mergeWith(emptyStats1);
        assertTrue(emptyStats2.empty());
        assertTrue(emptyStats1.empty());
        assertTrue(emptyStats1.getProducerNodeIds().isEmpty());
        assertTrue(emptyStats2.getProducerNodeIds().isEmpty());
    }

    @Test
    public void testJson()
    {
        JsonCodec<DynamicFilterStats> codec = JsonCodec.jsonCodec(DynamicFilterStats.class);
        final PlanNodeId[] planNodes = new PlanNodeId[] {new PlanNodeId("1"), new PlanNodeId("b")};
        DynamicFilterStats expect = new DynamicFilterStats(new HashSet<PlanNodeId>(Arrays.asList(planNodes)));

        String json = codec.toJson(expect);
        DynamicFilterStats actual = codec.fromJson(json);

        assertDynamicFilterStatsEquals(actual, expect);
    }
}
