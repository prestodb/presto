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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static org.testng.Assert.fail;

public class TestNodeMemoryConfig
{
    @Test
    public void testDefaults()
    {
        // This can't use assertRecordedDefaults because the default value is dependent on the current max heap size, which varies based on the current size of the survivor space.
        for (int i = 0; i < 1_000; i++) {
            DataSize expected = new DataSize(Runtime.getRuntime().maxMemory() * 0.1, BYTE);
            NodeMemoryConfig config = new NodeMemoryConfig();
            if (expected.equals(config.getMaxQueryMemoryPerNode())) {
                return;
            }
        }
        // We can't make this 100% deterministic, since we don't know when the survivor space will change sizes, but assume that something is broken if we got the wrong answer 1000 times
        fail();
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.max-memory-per-node", "1GB")
                .build();

        NodeMemoryConfig expected = new NodeMemoryConfig()
                .setMaxQueryMemoryPerNode(new DataSize(1, GIGABYTE));

        assertFullMapping(properties, expected);
    }
}
