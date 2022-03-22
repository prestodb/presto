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

public class TestNodeSchedulerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NodeSchedulerConfig.class)
                .setNetworkTopology(LEGACY)
                .setMinCandidates(10)
                .setMaxSplitsPerNode(100)
                .setMaxPendingSplitsPerTask(10)
                .setMaxUnacknowledgedSplitsPerTask(500)
                .setIncludeCoordinator(true)
                .setResourceAwareSchedulingStrategy(RANDOM));
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
                .put("experimental.resource-aware-scheduling-strategy", "TTL")
                .build();

        NodeSchedulerConfig expected = new NodeSchedulerConfig()
                .setNetworkTopology("flat")
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(101)
                .setMaxPendingSplitsPerTask(11)
                .setMaxUnacknowledgedSplitsPerTask(501)
                .setMinCandidates(11)
                .setResourceAwareSchedulingStrategy(TTL);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
