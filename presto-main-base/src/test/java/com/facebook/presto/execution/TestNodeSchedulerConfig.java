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
package com.facebook.presto.execution;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.LEGACY;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy.RANDOM;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy.TTL;
import static com.facebook.presto.execution.scheduler.NodeSelectionHashStrategy.CONSISTENT_HASHING;
import static com.facebook.presto.execution.scheduler.NodeSelectionHashStrategy.MODULAR_HASHING;

public class TestNodeSchedulerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NodeSchedulerConfig.class)
                .setNetworkTopology(LEGACY)
                .setMinCandidates(10)
                .setMaxSplitsPerNode(100)
                .setMaxSplitsPerTask(10)
                .setScheduleSplitsBasedOnTaskLoad(false)
                .setMaxPendingSplitsPerTask(10)
                .setMaxUnacknowledgedSplitsPerTask(500)
                .setIncludeCoordinator(true)
                .setNodeSelectionHashStrategy(MODULAR_HASHING)
                .setMinVirtualNodeCount(1000)
                .setResourceAwareSchedulingStrategy(RANDOM)
                .setMaxPreferredNodes(2));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("node-scheduler.network-topology", "flat")
                .put("node-scheduler.min-candidates", "11")
                .put("node-scheduler.include-coordinator", "false")
                .put("node-scheduler.max-pending-splits-per-task", "11")
                .put("node-scheduler.max-unacknowledged-splits-per-task", "501")
                .put("node-scheduler.max-splits-per-node", "101")
                .put("node-scheduler.max-splits-per-task", "17")
                .put("node-scheduler.schedule-splits-based-on-task-load", "true")
                .put("node-scheduler.node-selection-hash-strategy", "CONSISTENT_HASHING")
                .put("node-scheduler.consistent-hashing-min-virtual-node-count", "2000")
                .put("experimental.resource-aware-scheduling-strategy", "TTL")
                .put("node-scheduler.max-preferred-nodes", "5")
                .build();

        NodeSchedulerConfig expected = new NodeSchedulerConfig()
                .setNetworkTopology("flat")
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(101)
                .setMaxSplitsPerTask(17)
                .setScheduleSplitsBasedOnTaskLoad(true)
                .setMaxPendingSplitsPerTask(11)
                .setMaxUnacknowledgedSplitsPerTask(501)
                .setMinCandidates(11)
                .setNodeSelectionHashStrategy(CONSISTENT_HASHING)
                .setMinVirtualNodeCount(2000)
                .setResourceAwareSchedulingStrategy(TTL)
                .setMaxPreferredNodes(5);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
