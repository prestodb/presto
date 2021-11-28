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
package com.facebook.presto.kafka.server.file;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

public class TestFileKafkaClusterMetadataSupplierConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(FileKafkaClusterMetadataSupplierConfig.class)
                .setNodes(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.nodes", "localhost:12345,localhost:23456")
                .build();

        FileKafkaClusterMetadataSupplierConfig expected = new FileKafkaClusterMetadataSupplierConfig()
                .setNodes("localhost:12345,localhost:23456");
        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
