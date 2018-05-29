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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertNotNull;

public final class ElasticsearchQueryRunner
{
    private ElasticsearchQueryRunner() {}

    private static final Logger LOG = Logger.get(ElasticsearchQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";
    private static final int NODE_COUNT = 2;

    public static DistributedQueryRunner createElasticsearchQueryRunner(EmbeddedElasticsearchNode embeddedElasticsearch, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setNodeCount(NODE_COUNT)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            embeddedElasticsearch.start();

            ElasticsearchTableDescriptionProvider tableDescriptions = createTableDescriptions(queryRunner.getCoordinator().getMetadata(), tables);

            ElasticsearchConnectorTestFactory testFactory = new ElasticsearchConnectorTestFactory(tableDescriptions);

            installElasticsearchPlugin(queryRunner, tables, testFactory);

            TestingPrestoClient prestoClient = queryRunner.getClient();

            LOG.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTopic(embeddedElasticsearch, prestoClient, table);
            }
            LOG.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, embeddedElasticsearch);
            throw e;
        }
    }

    private static ElasticsearchTableDescriptionProvider createTableDescriptions(Metadata metadata, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        JsonCodec<ElasticsearchTableDescription> codec = new CodecSupplier<>(ElasticsearchTableDescription.class, metadata).get();

        URL metadataUrl = getResource(ElasticsearchQueryRunner.class, "/queryrunner");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadataUri = metadataUrl.toURI();

        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (TpchTable<?> table : tables) {
            tableNames.add(TPCH_SCHEMA + "." + table.getTableName());
        }

        ElasticsearchConnectorConfig config = new ElasticsearchConnectorConfig()
                .setTableDescriptionDir(new File(metadataUri))
                .setDefaultSchema(TPCH_SCHEMA)
                .setTableNames(Joiner.on(",").join(tableNames.build()));
        return new ElasticsearchTableDescriptionProvider(config, codec);
    }

    private static void installElasticsearchPlugin(QueryRunner queryRunner, Iterable<TpchTable<?>> tables, ElasticsearchConnectorTestFactory factory)
            throws Exception
    {
        ElasticsearchPlugin plugin = new ElasticsearchPlugin();
        plugin.setConnectorFactory(factory);
        queryRunner.installPlugin(plugin);

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (TpchTable<?> table : tables) {
            tableNames.add(new SchemaTableName(TPCH_SCHEMA, table.getTableName()));
        }

        URL metadataUrl = getResource(ElasticsearchQueryRunner.class, "/queryrunner");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadataUri = metadataUrl.toURI();
        Map<String, String> config = new HashMap<>();
        config.put("elasticsearch.default-schema", TPCH_SCHEMA);
        config.put("elasticsearch.table-names", Joiner.on(",").join(tableNames.build()));
        config.put("elasticsearch.table-description-dir", metadataUri.toString());
        config.put("elasticsearch.scroll-size", "1000");
        config.put("elasticsearch.scroll-timeout", "1m");
        config.put("elasticsearch.max-hits", "1000000");
        config.put("elasticsearch.request-timeout", "2m");

        queryRunner.createCatalog("elasticsearch", "elasticsearch", config);
    }

    private static void loadTpchTopic(EmbeddedElasticsearchNode embeddedElasticsearch, TestingPrestoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        LOG.info("Running import for %s", table.getTableName());
        ElasticsearchLoader loader = new ElasticsearchLoader(embeddedElasticsearch.getClient(), table.getTableName().toLowerCase(ENGLISH), prestoClient.getServer(), prestoClient.getDefaultSession());
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
