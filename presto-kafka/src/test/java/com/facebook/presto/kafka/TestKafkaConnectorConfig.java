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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

public class TestKafkaConnectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(KafkaConnectorConfig.class)
                .setNodes(null)
                .setKafkaConnectTimeout("10s")
                .setDefaultSchema("default")
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/kafka/"))
                .setHideInternalColumns(true)
                .setMaxPartitionFetchBytes(1048576)
                .setMaxPollRecords(500)
                .setZookeeperMaxRetries(3)
                .setZookeeperUri(null)
                .setZookeeperPath(null)
                .setZookeeperRetrySleepTime(100)
                .setDiscoveryMode("static"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.nodes", "localhost:9092")
                .put("kafka.table-description-dir", "/var/lib/kafka")
                .put("kafka.table-names", "table1, table2, table3")
                .put("kafka.default-schema", "kafka")
                .put("kafka.connect-timeout", "1h")
                .put("kafka.hide-internal-columns", "false")
                .put("kafka.max.partition.fetch.bytes", "1024")
                .put("kafka.max.poll.records", "1000")
                .put("kafka.zookeeper.max.retries", "5")
                .put("kafka.zookeeper.uri", "localhost1:2181")
                .put("kafka.zookeeper.path", "/zookeeper/path/")
                .put("kafka.zookeeper.retry.sleeptime", "200")
                .put("kafka.discovery.mode", "zookeeper")
                .build();

        KafkaConnectorConfig expected = new KafkaConnectorConfig()
                .setNodes("localhost:9092")
                .setTableDescriptionDir(new File("/var/lib/kafka"))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("kafka")
                .setKafkaConnectTimeout("1h")
                .setHideInternalColumns(false)
                .setMaxPartitionFetchBytes(1024)
                .setMaxPollRecords(1000)
                .setZookeeperMaxRetries(5)
                .setZookeeperUri("localhost1:2181")
                .setZookeeperPath("/zookeeper/path/")
                .setZookeeperRetrySleepTime(200)
                .setDiscoveryMode("zookeeper");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
