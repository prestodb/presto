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
package com.facebook.presto.elasticsearch;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;

import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class ElasticsearchQueryRunner
{
    private ElasticsearchQueryRunner() {}

    private static final Logger LOG = Logger.get(ElasticsearchQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";
    private static final int NODE_COUNT = 2;

    public static DistributedQueryRunner createElasticsearchQueryRunner(EmbeddedElasticsearchNode embeddedElasticsearchNode, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setNodeCount(NODE_COUNT)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            embeddedElasticsearchNode.start();

            TestingElasticsearchConnectorFactory testFactory = new TestingElasticsearchConnectorFactory();

            installElasticsearchPlugin(queryRunner, testFactory);

            TestingPrestoClient prestoClient = queryRunner.getRandomClient();

            LOG.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTopic(embeddedElasticsearchNode, prestoClient, table);
            }
            LOG.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, embeddedElasticsearchNode);
            throw e;
        }
    }

    private static void installElasticsearchPlugin(QueryRunner queryRunner, TestingElasticsearchConnectorFactory factory)
            throws Exception
    {
        queryRunner.installPlugin(new ElasticsearchPlugin(factory));
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("elasticsearch.host", "localhost")
                .put("elasticsearch.port", "9200")
                .put("elasticsearch.default-schema-name", TPCH_SCHEMA)
                .put("elasticsearch.scroll-size", "1000")
                .put("elasticsearch.scroll-timeout", "1m")
                .put("elasticsearch.max-hits", "1000000")
                .put("elasticsearch.request-timeout", "2m")
                .build();

        queryRunner.createCatalog("elasticsearch", "elasticsearch", config);
    }

    private static void loadTpchTopic(EmbeddedElasticsearchNode embeddedElasticsearchNode, TestingPrestoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        LOG.info("Running import for %s", table.getTableName());
        ElasticsearchLoader loader = new ElasticsearchLoader(embeddedElasticsearchNode.getClient(), table.getTableName().toLowerCase(ENGLISH), prestoClient.getServer(), prestoClient.getDefaultSession());
        loader.execute(format("SELECT * from %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
        LOG.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static Session createSession()
    {
        return testSessionBuilder().setCatalog("elasticsearch").setSchema(TPCH_SCHEMA).build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createElasticsearchQueryRunner(EmbeddedElasticsearchNode.createEmbeddedElasticsearchNode(), TpchTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(ElasticsearchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
