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
package com.facebook.presto.memory;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.airlift.units.DataSize;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static com.facebook.presto.memory.LocalMemoryManager.validateHeapHeadroom;
import static com.facebook.presto.memory.NodeMemoryConfig.AVAILABLE_HEAP_MEMORY;

public class TestNodeMemoryConfig
{
    private static final DataSize DEFAULT_MAX_QUERY_BROADCAST_MEMORY = new DataSize(AVAILABLE_HEAP_MEMORY * 0.1, BYTE);
    private static final DataSize DEFAULT_MAX_QUERY_MEMORY_PER_NODE = new DataSize(AVAILABLE_HEAP_MEMORY * 0.1, BYTE);
    private static final DataSize DEFAULT_SOFT_MAX_QUERY_MEMORY_PER_NODE = new DataSize(AVAILABLE_HEAP_MEMORY * 0.1, BYTE);
    private static final DataSize DEFAULT_MAX_QUERY_TOTAL_MEMORY_PER_NODE = new DataSize(AVAILABLE_HEAP_MEMORY * 0.2, BYTE);
    private static final DataSize DEFAULT_SOFT_MAX_QUERY_TOTAL_MEMORY_PER_NODE = new DataSize(AVAILABLE_HEAP_MEMORY * 0.2, BYTE);
    private static final DataSize DEFAULT_HEAP_HEADROOM = new DataSize(AVAILABLE_HEAP_MEMORY * 0.3, BYTE);

    private static final DataSize OVERRIDE_MAX_QUERY_BROADCAST_MEMORY = new DataSize(DEFAULT_MAX_QUERY_BROADCAST_MEMORY.toBytes() * 2, BYTE);
    private static final DataSize OVERRIDE_MAX_QUERY_MEMORY_PER_NODE = new DataSize(DEFAULT_MAX_QUERY_MEMORY_PER_NODE.toBytes() * 2, BYTE);
    private static final DataSize OVERRIDE_SOFT_MAX_QUERY_MEMORY_PER_NODE = new DataSize(DEFAULT_SOFT_MAX_QUERY_MEMORY_PER_NODE.toBytes() * 2, BYTE);
    private static final DataSize OVERRIDE_MAX_QUERY_TOTAL_MEMORY_PER_NODE = new DataSize(DEFAULT_MAX_QUERY_TOTAL_MEMORY_PER_NODE.toBytes() * 2, BYTE);
    private static final DataSize OVERRIDE_SOFT_MAX_QUERY_TOTAL_MEMORY_PER_NODE = new DataSize(DEFAULT_SOFT_MAX_QUERY_TOTAL_MEMORY_PER_NODE.toBytes() * 2, BYTE);
    private static final DataSize OVERRIDE_HEAP_HEADROOM = new DataSize(DEFAULT_HEAP_HEADROOM.toBytes() * 0.5, BYTE);

    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NodeMemoryConfig.class)
                .setMaxQueryBroadcastMemory(DEFAULT_MAX_QUERY_BROADCAST_MEMORY)
                .setMaxQueryMemoryPerNode(DEFAULT_MAX_QUERY_MEMORY_PER_NODE)
                .setSoftMaxQueryMemoryPerNode(DEFAULT_SOFT_MAX_QUERY_MEMORY_PER_NODE)
                .setMaxQueryTotalMemoryPerNode(DEFAULT_MAX_QUERY_TOTAL_MEMORY_PER_NODE)
                .setSoftMaxQueryTotalMemoryPerNode(DEFAULT_SOFT_MAX_QUERY_TOTAL_MEMORY_PER_NODE)
                .setHeapHeadroom(DEFAULT_HEAP_HEADROOM)
                .setReservedPoolEnabled(true)
                .setVerboseExceededMemoryLimitErrorsEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.max-memory-per-node", OVERRIDE_MAX_QUERY_MEMORY_PER_NODE.toString())
                .put("query.max-broadcast-memory", OVERRIDE_MAX_QUERY_BROADCAST_MEMORY.toString())
                .put("query.soft-max-memory-per-node", OVERRIDE_SOFT_MAX_QUERY_MEMORY_PER_NODE.toString())
                .put("query.max-total-memory-per-node", OVERRIDE_MAX_QUERY_TOTAL_MEMORY_PER_NODE.toString())
                .put("query.soft-max-total-memory-per-node", OVERRIDE_SOFT_MAX_QUERY_TOTAL_MEMORY_PER_NODE.toString())
                .put("memory.heap-headroom-per-node", OVERRIDE_HEAP_HEADROOM.toString())
                .put("experimental.reserved-pool-enabled", "false")
                .put("memory.verbose-exceeded-memory-limit-errors-enabled", "false")
                .build();

        NodeMemoryConfig expected = new NodeMemoryConfig()
                .setMaxQueryBroadcastMemory(OVERRIDE_MAX_QUERY_BROADCAST_MEMORY)
                .setMaxQueryMemoryPerNode(OVERRIDE_MAX_QUERY_MEMORY_PER_NODE)
                .setSoftMaxQueryMemoryPerNode(OVERRIDE_SOFT_MAX_QUERY_MEMORY_PER_NODE)
                .setMaxQueryTotalMemoryPerNode(OVERRIDE_MAX_QUERY_TOTAL_MEMORY_PER_NODE)
                .setSoftMaxQueryTotalMemoryPerNode(OVERRIDE_SOFT_MAX_QUERY_TOTAL_MEMORY_PER_NODE)
                .setHeapHeadroom(OVERRIDE_HEAP_HEADROOM)
                .setReservedPoolEnabled(false)
                .setVerboseExceededMemoryLimitErrorsEnabled(false);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testOutOfRangeBroadcastMemoryLimit()
    {
        DataSize invalidBroadcastMemory = new DataSize(OVERRIDE_MAX_QUERY_MEMORY_PER_NODE.toBytes() * 2, BYTE);
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.max-memory-per-node", OVERRIDE_MAX_QUERY_MEMORY_PER_NODE.toString())
                .put("query.max-broadcast-memory", invalidBroadcastMemory.toString())
                .put("query.soft-max-memory-per-node", OVERRIDE_SOFT_MAX_QUERY_MEMORY_PER_NODE.toString())
                .put("query.max-total-memory-per-node", OVERRIDE_MAX_QUERY_TOTAL_MEMORY_PER_NODE.toString())
                .put("query.soft-max-total-memory-per-node", OVERRIDE_SOFT_MAX_QUERY_TOTAL_MEMORY_PER_NODE.toString())
                .put("memory.heap-headroom-per-node", OVERRIDE_HEAP_HEADROOM.toString())
                .put("experimental.reserved-pool-enabled", "false")
                .put("memory.verbose-exceeded-memory-limit-errors-enabled", "false")
                .build();

        NodeMemoryConfig expected = new NodeMemoryConfig()
                .setMaxQueryMemoryPerNode(OVERRIDE_MAX_QUERY_MEMORY_PER_NODE)
                .setMaxQueryBroadcastMemory(OVERRIDE_MAX_QUERY_MEMORY_PER_NODE)  // broadcast memory not allowed to exceed this value
                .setSoftMaxQueryMemoryPerNode(OVERRIDE_SOFT_MAX_QUERY_MEMORY_PER_NODE)
                .setMaxQueryTotalMemoryPerNode(OVERRIDE_MAX_QUERY_TOTAL_MEMORY_PER_NODE)
                .setSoftMaxQueryTotalMemoryPerNode(OVERRIDE_SOFT_MAX_QUERY_TOTAL_MEMORY_PER_NODE)
                .setHeapHeadroom(OVERRIDE_HEAP_HEADROOM)
                .setReservedPoolEnabled(false)
                .setVerboseExceededMemoryLimitErrorsEnabled(false);

        assertFullMapping(properties, expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidValues()
    {
        NodeMemoryConfig config = new NodeMemoryConfig();
        config.setMaxQueryTotalMemoryPerNode(new DataSize(1, GIGABYTE));
        config.setHeapHeadroom(new DataSize(3.1, GIGABYTE));
        // In this case we have 4GB - 1GB = 3GB available memory for the general pool
        // and the heap headroom and the config is more than that.
        validateHeapHeadroom(config, new DataSize(4, GIGABYTE).toBytes());
    }
}
