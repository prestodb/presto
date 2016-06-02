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
package com.facebook.presto.accumulo;

import com.facebook.presto.Session;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.io.Text;
import org.intellij.lang.annotations.Language;

import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class AccumuloQueryRunner
{
    private static final Logger LOG = Logger.get(AccumuloQueryRunner.class);

    private AccumuloQueryRunner()
    {}

    public static DistributedQueryRunner createAccumuloQueryRunner(
            Map<String, String> extraProperties, boolean loadTpch)
            throws Exception
    {
        DistributedQueryRunner queryRunner =
                new DistributedQueryRunner(createSession(), 2, extraProperties);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new AccumuloPlugin());
        Map<String, String> accumuloProperties =
                ImmutableMap.<String, String>builder()
                        .put(AccumuloConfig.INSTANCE, "default")
                        .put(AccumuloConfig.ZOOKEEPERS, "localhost:2181")
                        .put(AccumuloConfig.USERNAME, "root")
                        .put(AccumuloConfig.PASSWORD, "secret")
                        .put(AccumuloConfig.ZOOKEEPER_METADATA_ROOT, "/presto-accumulo-test")
                        .put(AccumuloConfig.MINI_ACCUMULO_CLUSTER, "true")
                        .build();

        queryRunner.createCatalog("accumulo", "accumulo", accumuloProperties);

        if (loadTpch) {
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), TpchTable.getTables());
            Connector conn = AccumuloClient.getAccumuloConnector(new AccumuloConfig());
            conn.tableOperations().addSplits("tpch.orders",
                    ImmutableSortedSet.of(new Text(new LexicoderRowSerializer().encode(BIGINT, 7500L))));
        }

        return queryRunner;
    }

    private static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        LOG.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, session, sourceSchema, table);
        }
        LOG.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    public static void dropTpchTables(QueryRunner queryRunner, Session session)
    {
        for (TpchTable<?> table : TpchTable.getTables()) {
            @Language("SQL")
            String sql = format("DROP TABLE IF EXISTS %s", table.getTableName());
            LOG.info("%s", sql);
            queryRunner.execute(session, sql);
        }
    }

    private static void copyTable(QueryRunner queryRunner, String catalog, Session session,
            String schema, TpchTable<?> table)
    {
        QualifiedObjectName source = new QualifiedObjectName(catalog, schema, table.getTableName());
        String target = table.getTableName();

        @Language("SQL")
        String sql;
        switch (target) {
            case "customer":
                sql = format("CREATE TABLE %s WITH (index_columns = 'mktsegment') AS SELECT * FROM %s", target, source);
                break;
            case "lineitem":
                sql = format("CREATE TABLE %s WITH (index_columns = 'quantity,discount,returnflag,shipdate,receiptdate,shipinstruct,shipmode') AS SELECT UUID() AS uuid, * FROM %s", target, source);
                break;
            case "orders":
                sql = format("CREATE TABLE %s WITH (index_columns = 'orderdate') AS SELECT * FROM %s", target, source);
                break;
            case "part":
                sql = format("CREATE TABLE %s WITH (index_columns = 'brand,type,size,container') AS SELECT * FROM %s", target, source);
                break;
            case "partsupp":
                sql = format("CREATE TABLE %s WITH (index_columns = 'partkey') AS SELECT UUID() AS uuid, * FROM %s", target, source);
                break;
            case "supplier":
                sql = format("CREATE TABLE %s WITH (index_columns = 'name') AS SELECT * FROM %s", target, source);
                break;
            default:
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", target, source);
                break;
        }

        LOG.info("Running import for %s", target, sql);
        LOG.info("%s", sql);
        long start = System.nanoTime();
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        LOG.info("Imported %s rows for %s in %s", rows, target, nanosSince(start));
    }

    public static Session createSession()
    {
        return testSessionBuilder().setCatalog("accumulo").setSchema("tpch").build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = createAccumuloQueryRunner(properties, false);
        Thread.sleep(10);
        Logger log = Logger.get(AccumuloQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
