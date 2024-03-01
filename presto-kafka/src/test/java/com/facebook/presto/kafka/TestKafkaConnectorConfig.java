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
package com.facebook.presto.kafka;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.kafka.schema.file.FileTableDescriptionSupplier;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

public class TestKafkaConnectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(KafkaConnectorConfig.class)
                .setKafkaConnectTimeout("10s")
                .setDefaultSchema("default")
                .setClusterMetadataSupplier(FileTableDescriptionSupplier.NAME)
                .setTableDescriptionSupplier(FileTableDescriptionSupplier.NAME)
                .setHideInternalColumns(true)
                .setMaxPartitionFetchBytes(1048576)
                .setMaxPollRecords(500));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.table-description-supplier", "test")
                .put("kafka.cluster-metadata-supplier", "test")
                .put("kafka.default-schema", "kafka")
                .put("kafka.connect-timeout", "1h")
                .put("kafka.hide-internal-columns", "false")
                .put("kafka.max-partition-fetch-bytes", "1024")
                .put("kafka.max-poll-records", "1000")
                .build();

        KafkaConnectorConfig expected = new KafkaConnectorConfig()
                .setTableDescriptionSupplier("test")
                .setClusterMetadataSupplier("test")
                .setDefaultSchema("kafka")
                .setKafkaConnectTimeout("1h")
                .setHideInternalColumns(false)
                .setMaxPartitionFetchBytes(1024)
                .setMaxPollRecords(1000);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
