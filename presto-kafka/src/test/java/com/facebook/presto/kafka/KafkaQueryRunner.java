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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.decoder.DecoderModule;
import com.facebook.presto.kafka.encoder.EncoderModule;
import com.facebook.presto.kafka.schema.MapBasedTableDescriptionSupplier;
import com.facebook.presto.kafka.schema.TableDescriptionSupplier;
import com.facebook.presto.kafka.server.KafkaClusterMetadataSupplier;
import com.facebook.presto.kafka.server.file.FileKafkaClusterMetadataSupplier;
import com.facebook.presto.kafka.server.file.FileKafkaClusterMetadataSupplierConfig;
import com.facebook.presto.kafka.util.CodecSupplier;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.kafka.util.TestUtils;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.tpch.TpchTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.presto.kafka.ConfigurationAwareModules.combine;
import static com.facebook.presto.kafka.util.TestUtils.loadTpchTopicDescription;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.io.ByteStreams.toByteArray;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class KafkaQueryRunner
{
    private KafkaQueryRunner()
    {
    }

    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SCHEMA = "tpch";
    private static final String TEST = "test";

    public static Builder builder(EmbeddedKafka embeddedKafka)
    {
        return new Builder(embeddedKafka);
    }

    public static class Builder
            extends KafkaQueryRunnerBuilder
    {
        private List<TpchTable<?>> tables = ImmutableList.of();
        private Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription = ImmutableMap.of();

        protected Builder(EmbeddedKafka embeddedKafka)
        {
            super(embeddedKafka, TPCH_SCHEMA);
        }

        public Builder setTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
            return this;
        }

        public Builder setExtraTopicDescription(Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription)
        {
            this.extraTopicDescription = ImmutableMap.copyOf(requireNonNull(extraTopicDescription, "extraTopicDescription is null"));
            return this;
        }

        @Override
        public Builder setExtension(Module extension)
        {
            this.extension = requireNonNull(extension, "extension is null");
            return this;
        }

        @Override
        public void preInit(DistributedQueryRunner queryRunner)
                throws Exception
        {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            List<SchemaTableName> tableNames = new ArrayList<>();
            tableNames.add(new SchemaTableName("read_test", "all_datatypes_json"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_avro"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_csv"));

            JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, queryRunner.getMetadata()).get();

            ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> testTopicDescriptions = ImmutableMap.builder();
            for (SchemaTableName tableName : tableNames) {
                embeddedKafka.createTopics(String.format("%s.%s", tableName.getSchemaName(), tableName.getTableName()));
                testTopicDescriptions.put(tableName, createTable(tableName, topicDescriptionJsonCodec));
            }

            for (TpchTable<?> table : tables) {
                embeddedKafka.createTopics(kafkaTopicName(table));
            }
            Map<SchemaTableName, KafkaTopicDescription> tpchTopicDescriptions = createTpchTopicDescriptions(queryRunner.getCoordinator().getMetadata(), tables);

            FileKafkaClusterMetadataSupplierConfig clusterMetadataSupplierConfig = new FileKafkaClusterMetadataSupplierConfig();
            clusterMetadataSupplierConfig.setNodes(embeddedKafka.getConnectString());
            Map<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                    .putAll(extraTopicDescription)
                    .putAll(tpchTopicDescriptions)
                    .putAll(testTopicDescriptions.build())
                    .build();
            setExtension(combine(
                    installModuleIf(
                            KafkaConnectorConfig.class,
                            kafkaConfig -> kafkaConfig.getTableDescriptionSupplier().equalsIgnoreCase(TEST),
                            binder -> binder.bind(TableDescriptionSupplier.class)
                                    .toInstance(new MapBasedTableDescriptionSupplier(topicDescriptions))),
                    installModuleIf(
                            KafkaConnectorConfig.class,
                            kafkaConfig -> kafkaConfig.getClusterMetadataSupplier().equalsIgnoreCase(TEST),
                            binder -> binder.bind(KafkaClusterMetadataSupplier.class)
                                    .toInstance(new FileKafkaClusterMetadataSupplier(clusterMetadataSupplierConfig))),
                    new DecoderModule(),
                    new EncoderModule()));
            Map<String, String> properties = new HashMap<>(extraKafkaProperties);
            properties.putIfAbsent("kafka.table-description-supplier", TEST);
            setExtraKafkaProperties(properties);
        }

        @Override
        public void postInit(DistributedQueryRunner queryRunner)
        {
            log.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                long start = System.nanoTime();
                loadTpchTopic(embeddedKafka, queryRunner.getRandomClient(), table);
                log.info("Imported %s in %s", 0, table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
            }
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
        }
    }

    private static void loadTpchTopic(EmbeddedKafka embeddedKafka, TestingPrestoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        TestUtils.loadTpchTopic(embeddedKafka, prestoClient, kafkaTopicName(table), new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)));
        log.info("Imported %s in %s", 0, table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String kafkaTopicName(TpchTable<?> table)
    {
        return TPCH_SCHEMA + "." + table.getTableName().toLowerCase(ENGLISH);
    }

    private static Map<SchemaTableName, KafkaTopicDescription> createTpchTopicDescriptions(Metadata metadata, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, metadata).get();

        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            SchemaTableName tpchTable = new SchemaTableName(TPCH_SCHEMA, tableName);
            topicDescriptions.put(loadTpchTopicDescription(topicDescriptionJsonCodec, tpchTable.toString(), tpchTable));
        }

        return topicDescriptions.build();
    }

    private static KafkaTopicDescription createTable(SchemaTableName table, JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec)
            throws IOException
    {
        String fileName = format("/%s/%s.json", table.getSchemaName(), table.getTableName());
        KafkaTopicDescription tableTemplate = topicDescriptionJsonCodec.fromJson(toByteArray(KafkaQueryRunner.class.getResourceAsStream(fileName)));
        Optional<KafkaTopicFieldGroup> key = tableTemplate.getKey()
                .map(keyTemplate -> new KafkaTopicFieldGroup(
                        keyTemplate.getDataFormat(),
                        keyTemplate.getDataSchema().map(schema -> KafkaQueryRunner.class.getResource(schema).getPath()),
                        keyTemplate.getFields()));

        Optional<KafkaTopicFieldGroup> message = tableTemplate.getMessage()
                .map(keyTemplate -> new KafkaTopicFieldGroup(
                        keyTemplate.getDataFormat(),
                        keyTemplate.getDataSchema().map(schema -> KafkaQueryRunner.class.getResource(schema).getPath()),
                        keyTemplate.getFields()));

        return new KafkaTopicDescription(
                table.getTableName(),
                Optional.of(table.getSchemaName()),
                table.toString(),
                key,
                message);
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = builder(EmbeddedKafka.createEmbeddedKafka())
                .setTables(TpchTable.getTables())
                .build();
        Thread.sleep(10);
        Logger log = Logger.get(KafkaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
