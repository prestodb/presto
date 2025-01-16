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
package com.facebook.presto.spiller;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestNodeSpillConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(NodeSpillConfig.class)
                .setMaxSpillPerNode(new DataSize(100, GIGABYTE))
                .setMaxRevocableMemoryPerNode(new DataSize(16, GIGABYTE))
                .setQueryMaxSpillPerNode(new DataSize(100, GIGABYTE))
                .setSpillCompressionEnabled(false)
                .setSpillEncryptionEnabled(false)
                .setTempStorageBufferSize(new DataSize(4, KILOBYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("experimental.max-spill-per-node", "10MB")
                .put("experimental.max-revocable-memory-per-node", "24MB")
                .put("experimental.query-max-spill-per-node", "15 MB")
                .put("experimental.spill-compression-enabled", "true")
                .put("experimental.spill-encryption-enabled", "true")
                .put("experimental.temp-storage-buffer-size", "24MB")
                .build();

        NodeSpillConfig expected = new NodeSpillConfig()
                .setMaxSpillPerNode(new DataSize(10, MEGABYTE))
                .setMaxRevocableMemoryPerNode(new DataSize(24, MEGABYTE))
                .setQueryMaxSpillPerNode(new DataSize(15, MEGABYTE))
                .setSpillCompressionEnabled(true)
                .setSpillEncryptionEnabled(true)
                .setTempStorageBufferSize(new DataSize(24, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
