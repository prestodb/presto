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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.kafka.util.CodecSupplier;
import io.prestosql.plugin.kafka.util.EmbeddedKafka;
import io.prestosql.plugin.kafka.util.TestUtils;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.tests.DistributedQueryRunner;
import io.prestosql.tests.TestingPrestoClient;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.kafka.util.TestUtils.installKafkaPlugin;
import static io.prestosql.plugin.kafka.util.TestUtils.loadTpchTopicDescription;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class KafkaQueryRunner
{
    private KafkaQueryRunner()
    {
    }

    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createKafkaQueryRunner(EmbeddedKafka embeddedKafka, TpchTable<?>... tables)
            throws Exception
    {
        return createKafkaQueryRunner(embeddedKafka, ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createKafkaQueryRunner(EmbeddedKafka embeddedKafka, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 2);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            embeddedKafka.start();

            for (TpchTable<?> table : tables) {
                embeddedKafka.createTopics(kafkaTopicName(table));
            }

            Map<SchemaTableName, KafkaTopicDescription> topicDescriptions = createTpchTopicDescriptions(queryRunner.getCoordinator().getMetadata(), tables);

            installKafkaPlugin(embeddedKafka, queryRunner, topicDescriptions);

            TestingPrestoClient prestoClient = queryRunner.getClient();

            log.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTopic(embeddedKafka, prestoClient, table);
            }
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, embeddedKafka);
            throw e;
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

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("kafka")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createKafkaQueryRunner(EmbeddedKafka.createEmbeddedKafka(), TpchTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(KafkaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
