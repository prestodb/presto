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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.kafka.KafkaConnectorConfig;
import com.facebook.presto.kafka.KafkaPlugin;
import com.facebook.presto.kafka.KafkaTopicDescription;
import com.facebook.presto.kafka.KafkaTopicFieldDescription;
import com.facebook.presto.kafka.KafkaTopicFieldGroup;
import com.facebook.presto.kafka.schema.MapBasedTableDescriptionSupplier;
import com.facebook.presto.kafka.schema.TableDescriptionSupplier;
import com.facebook.presto.kafka.server.KafkaClusterMetadataSupplier;
import com.facebook.presto.kafka.server.file.FileKafkaClusterMetadataSupplier;
import com.facebook.presto.kafka.server.file.FileKafkaClusterMetadataSupplierConfig;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.presto.kafka.ConfigurationAwareModules.combine;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;

public final class TestUtils
{
    private static final String TEST = "test";

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

    public static void installKafkaPlugin(EmbeddedKafka embeddedKafka, QueryRunner queryRunner, Map<SchemaTableName, KafkaTopicDescription> topicDescriptions, Map<String, String> connectorProperties)
    {
        FileKafkaClusterMetadataSupplierConfig clusterMetadataSupplierConfig = new FileKafkaClusterMetadataSupplierConfig();
        clusterMetadataSupplierConfig.setNodes(embeddedKafka.getConnectString());
        KafkaPlugin kafkaPlugin = new KafkaPlugin(combine(
                installModuleIf(
                        KafkaConnectorConfig.class,
                        kafkaConfig -> kafkaConfig.getTableDescriptionSupplier().equalsIgnoreCase(TEST),
                        binder -> binder.bind(TableDescriptionSupplier.class)
                                .toInstance(new MapBasedTableDescriptionSupplier(topicDescriptions))),
                installModuleIf(
                        KafkaConnectorConfig.class,
                        kafkaConfig -> kafkaConfig.getClusterMetadataSupplier().equalsIgnoreCase(TEST),
                        binder -> binder.bind(KafkaClusterMetadataSupplier.class)
                                .toInstance(new FileKafkaClusterMetadataSupplier(clusterMetadataSupplierConfig)))));

        queryRunner.installPlugin(kafkaPlugin);

        ImmutableMap.Builder<String, String> kafkaConfigBuilder = ImmutableMap.builder();
        kafkaConfigBuilder.put("kafka.cluster-metadata-supplier", TEST);
        kafkaConfigBuilder.put("kafka.table-description-supplier", TEST);
        kafkaConfigBuilder.put("kafka.connect-timeout", "120s");
        kafkaConfigBuilder.put("kafka.default-schema", "default");

        kafkaConfigBuilder.putAll(connectorProperties);

        queryRunner.createCatalog("kafka", "kafka", kafkaConfigBuilder.build());
    }

    public static void installKafkaPlugin(EmbeddedKafka embeddedKafka, QueryRunner queryRunner, Map<SchemaTableName, KafkaTopicDescription> topicDescriptions)
    {
        installKafkaPlugin(embeddedKafka, queryRunner, topicDescriptions, ImmutableMap.of());
    }

    public static void loadTpchTopic(EmbeddedKafka embeddedKafka, TestingPrestoClient prestoClient, String topicName, QualifiedObjectName tpchTableName)
    {
        try (KafkaProducer<Long, Object> producer = embeddedKafka.createProducer();
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

    public static Map<SchemaTableName, KafkaTopicDescription> createEmptyTableDescriptions(SchemaTableName... tableNames)
    {
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();
        for (SchemaTableName tableName : tableNames) {
            builder.put(tableName, new KafkaTopicDescription(
                    tableName.getTableName(),
                    Optional.of(tableName.getSchemaName()),
                    tableName.getSchemaName() + "." + tableName.getTableName(),
                    Optional.empty(),
                    Optional.empty()));
        }
        return builder.build();
    }

    public static Map<SchemaTableName, KafkaTopicDescription> createTableDescriptionsWithColumns(
            Map<SchemaTableName, List<KafkaTopicFieldDescription>> tableColumns)
    {
        return tableColumns.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> {
                            SchemaTableName tableName = entry.getKey();
                            List<KafkaTopicFieldDescription> columns = entry.getValue();
                            return new KafkaTopicDescription(
                                    tableName.getTableName(),
                                    Optional.of(tableName.getSchemaName()),
                                    tableName.getSchemaName() + "." + tableName.getTableName(),
                                    Optional.empty(),
                                    Optional.of(new KafkaTopicFieldGroup("json", Optional.empty(), columns)));
                        }));
    }
}
