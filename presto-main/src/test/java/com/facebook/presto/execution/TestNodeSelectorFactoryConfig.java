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

import com.facebook.presto.execution.scheduler.NodeSelectorFactoryConfig;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.execution.scheduler.NodeSelectorFactoryConfig.NetworkTopologyType.LEGACY;

public class TestNodeSelectorFactoryConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NodeSelectorFactoryConfig.class)
                .setNetworkTopology(LEGACY)
                .setMinCandidates(10)
                .setMaxSplitsPerNode(100)
                .setMaxPendingSplitsPerTask(10)
                .setIncludeCoordinator(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("node-scheduler.network-topology", "flat")
                .put("node-scheduler.min-candidates", "11")
                .put("node-scheduler.include-coordinator", "false")
                .put("node-scheduler.max-pending-splits-per-task", "11")
                .put("node-scheduler.max-splits-per-node", "101")
                .build();

        NodeSelectorFactoryConfig expected = new NodeSelectorFactoryConfig()
                .setNetworkTopology("flat")
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(101)
                .setMaxPendingSplitsPerTask(11)
                .setMinCandidates(11);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
