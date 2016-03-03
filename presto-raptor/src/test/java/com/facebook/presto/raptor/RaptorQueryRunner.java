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
package com.facebook.presto.raptor;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.tpch.testing.SampledTpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;

public final class RaptorQueryRunner
{
    private static final Logger log = Logger.get(RaptorQueryRunner.class);

    private RaptorQueryRunner() {}

    public static DistributedQueryRunner createRaptorQueryRunner(Map<String, String> extraProperties, boolean loadTpch, boolean bucketed)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 2, extraProperties);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new SampledTpchPlugin());
        queryRunner.createCatalog("tpch_sampled", "tpch_sampled");

        queryRunner.installPlugin(new RaptorPlugin());
        File baseDir = queryRunner.getCoordinator().getBaseDataDir().toFile();
        Map<String, String> raptorProperties = ImmutableMap.<String, String>builder()
                .put("metadata.db.type", "h2")
                .put("metadata.db.connections.max", "100")
                .put("metadata.db.filename", new File(baseDir, "db").getAbsolutePath())
                .put("storage.data-directory", new File(baseDir, "data").getAbsolutePath())
                .put("storage.max-shard-rows", "2000")
                .put("backup.provider", "file")
                .put("backup.directory", new File(baseDir, "backup").getAbsolutePath())
                .build();

        queryRunner.createCatalog("raptor", "raptor", raptorProperties);

        if (loadTpch) {
            copyTables(queryRunner, "tpch", createSession(), bucketed);
            copyTables(queryRunner, "tpch_sampled", createSampledSession(), bucketed);
        }

        return queryRunner;
    }

    private static void copyTables(QueryRunner queryRunner, String catalog, Session session, boolean bucketed)
            throws Exception
    {
        String schema = TINY_SCHEMA_NAME;
        if (!bucketed) {
            copyTpchTables(queryRunner, catalog, schema, session, TpchTable.getTables());
            return;
        }

        Map<TpchTable<?>, String> tables = ImmutableMap.<TpchTable<?>, String>builder()
                .put(TpchTable.ORDERS, "bucket_count = 25, bucketed_on = ARRAY['orderkey'], distribution_name = 'order'")
                .put(TpchTable.LINE_ITEM, "bucket_count = 25, bucketed_on = ARRAY['orderkey'], distribution_name = 'order'")
                .put(TpchTable.PART, "bucket_count = 20, bucketed_on = ARRAY['partkey'], distribution_name = 'part'")
                .put(TpchTable.PART_SUPPLIER, "bucket_count = 20, bucketed_on = ARRAY['partkey'], distribution_name = 'part'")
                .put(TpchTable.SUPPLIER, "bucket_count = 10, bucketed_on = ARRAY['suppkey']")
                .put(TpchTable.CUSTOMER, "bucket_count = 10, bucketed_on = ARRAY['custkey']")
                .put(TpchTable.NATION, "")
                .put(TpchTable.REGION, "")
                .build();

        log.info("Loading data from %s.%s...", catalog, schema);
        long startTime = System.nanoTime();
        for (Entry<TpchTable<?>, String> entry : tables.entrySet()) {
            copyTable(queryRunner, catalog, session, schema, entry.getKey(), entry.getValue());
        }
        log.info("Loading from %s.%s complete in %s", catalog, schema, nanosSince(startTime));
    }

    private static void copyTable(QueryRunner queryRunner, String catalog, Session session, String schema, TpchTable<?> table, String properties)
    {
        QualifiedObjectName source = new QualifiedObjectName(catalog, schema, table.getTableName());
        String target = table.getTableName();

        String with = properties.isEmpty() ? "" : format(" WITH (%s)", properties);
        @Language("SQL") String sql = format("CREATE TABLE %s%s AS SELECT * FROM %s", target, with, source);

        log.info("Running import for %s", target);
        long start = System.nanoTime();
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        log.info("Imported %s rows for %s in %s", rows, target, nanosSince(start));
    }

    public static Session createSession()
    {
        return createSession("tpch");
    }

    public static Session createSampledSession()
    {
        return createSession("tpch_sampled");
    }

    private static Session createSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("raptor")
                .setSchema(schema)
                .setSystemProperties(ImmutableMap.of("columnar_processing_dictionary", "true", "dictionary_aggregation", "true"))
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = createRaptorQueryRunner(properties, false, false);
        Thread.sleep(10);
        Logger log = Logger.get(RaptorQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
