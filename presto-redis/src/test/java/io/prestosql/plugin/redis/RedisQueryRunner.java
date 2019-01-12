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
package io.prestosql.plugin.redis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.redis.util.CodecSupplier;
import io.prestosql.plugin.redis.util.EmbeddedRedis;
import io.prestosql.plugin.redis.util.RedisTestUtils;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.tests.DistributedQueryRunner;
import io.prestosql.tests.TestingPrestoClient;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static io.prestosql.plugin.redis.util.RedisTestUtils.loadTpchTableDescription;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class RedisQueryRunner
{
    private RedisQueryRunner()
    {
    }

    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createRedisQueryRunner(EmbeddedRedis embeddedRedis, String dataFormat, TpchTable<?>... tables)
            throws Exception
    {
        return createRedisQueryRunner(embeddedRedis, dataFormat, ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createRedisQueryRunner(EmbeddedRedis embeddedRedis, String dataFormat, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 2);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            embeddedRedis.start();

            Map<SchemaTableName, RedisTableDescription> tableDescriptions = createTpchTableDescriptions(queryRunner.getCoordinator().getMetadata(), tables, dataFormat);

            installRedisPlugin(embeddedRedis, queryRunner, tableDescriptions);

            TestingPrestoClient prestoClient = queryRunner.getClient();

            log.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTable(embeddedRedis, prestoClient, table, dataFormat);
            }
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
            embeddedRedis.destroyJedisPool();
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, embeddedRedis);
            throw e;
        }
    }

    private static void loadTpchTable(EmbeddedRedis embeddedRedis, TestingPrestoClient prestoClient, TpchTable<?> table, String dataFormat)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        RedisTestUtils.loadTpchTable(
                embeddedRedis,
                prestoClient,
                redisTableName(table),
                new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)),
                dataFormat);
        log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String redisTableName(TpchTable<?> table)
    {
        return TPCH_SCHEMA + ":" + table.getTableName().toLowerCase(ENGLISH);
    }

    private static Map<SchemaTableName, RedisTableDescription> createTpchTableDescriptions(Metadata metadata, Iterable<TpchTable<?>> tables, String dataFormat)
            throws Exception
    {
        JsonCodec<RedisTableDescription> tableDescriptionJsonCodec = new CodecSupplier<>(RedisTableDescription.class, metadata).get();

        ImmutableMap.Builder<SchemaTableName, RedisTableDescription> tableDescriptions = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            SchemaTableName tpchTable = new SchemaTableName(TPCH_SCHEMA, tableName);

            tableDescriptions.put(loadTpchTableDescription(tableDescriptionJsonCodec, tpchTable, dataFormat));
        }
        return tableDescriptions.build();
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("redis")
                .setSchema(TPCH_SCHEMA)
                .build();
    }
}
