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
package com.facebook.presto.kafka.util;

import com.facebook.presto.kafka.KafkaPlugin;
import com.facebook.presto.kafka.KafkaTopicDescription;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.kafka.util.EmbeddedKafka.CloseableProducer;
import static java.lang.String.format;

public final class TestUtils
{
    private TestUtils() {}

    public static int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public static void installKafkaPlugin(EmbeddedKafka embeddedKafka, QueryRunner queryRunner, Map<SchemaTableName, KafkaTopicDescription> topicDescriptions)
    {
        KafkaPlugin kafkaPlugin = new KafkaPlugin();
        kafkaPlugin.setTableDescriptionSupplier(() -> topicDescriptions);
        queryRunner.installPlugin(kafkaPlugin);

        Map<String, String> kafkaConfig = ImmutableMap.of(
                "kafka.nodes", embeddedKafka.getConnectString(),
                "kafka.table-names", Joiner.on(",").join(topicDescriptions.keySet()),
                "kafka.connect-timeout", "120s",
                "kafka.default-schema", "default");
        queryRunner.createCatalog("kafka", "kafka", kafkaConfig);
    }

    public static void loadTpchTopic(EmbeddedKafka embeddedKafka, TestingPrestoClient prestoClient, String topicName, QualifiedObjectName tpchTableName)
    {
        try (CloseableProducer<Long, Object> producer = embeddedKafka.createProducer();
                KafkaLoader tpchLoader = new KafkaLoader(producer, topicName, prestoClient.getServer(), prestoClient.getDefaultSession())) {
            tpchLoader.execute(format("SELECT * from %s", tpchTableName));
        }
    }

    public static Map.Entry<SchemaTableName, KafkaTopicDescription> loadTpchTopicDescription(JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec, String topicName, SchemaTableName schemaTableName)
            throws IOException
    {
        KafkaTopicDescription tpchTemplate = topicDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(TestUtils.class.getResourceAsStream(format("/tpch/%s.json", schemaTableName.getTableName()))));

        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new KafkaTopicDescription(schemaTableName.getTableName(), Optional.of(schemaTableName.getSchemaName()), topicName, tpchTemplate.getKey(), tpchTemplate.getMessage()));
    }

    public static Map.Entry<SchemaTableName, KafkaTopicDescription> createEmptyTopicDescription(String topicName, SchemaTableName schemaTableName)
    {
        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new KafkaTopicDescription(schemaTableName.getTableName(), Optional.of(schemaTableName.getSchemaName()), topicName, Optional.empty(), Optional.empty()));
    }
}
