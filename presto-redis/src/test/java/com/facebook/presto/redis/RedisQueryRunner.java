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
package com.facebook.presto.redis;

import com.facebook.presto.Session;
import com.facebook.presto.redis.util.CodecSupplier;
import com.facebook.presto.redis.util.EmbeddedRedis;
import com.facebook.presto.redis.util.TestUtils;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;

import java.util.Map;

import static com.facebook.presto.redis.util.TestUtils.installRedisPlugin;
import static com.facebook.presto.redis.util.TestUtils.loadTpchTableDescription;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static io.airlift.testing.Closeables.closeAllSuppress;

public final class RedisQueryRunner
{
    private RedisQueryRunner()
    {
    }

    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SCHEMA = "tpch";
    public static String dataFormat = "string";

    public static DistributedQueryRunner createRedisQueryRunner(EmbeddedRedis embeddedRedis, String dataFormat, TpchTable<?>... tables)
            throws Exception
    {
        return createRedisQueryRunner(embeddedRedis, dataFormat, ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createRedisQueryRunner(EmbeddedRedis embeddedRedis, String dataFormat, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        RedisQueryRunner.dataFormat = dataFormat;

        try {
            queryRunner = new DistributedQueryRunner(createSession(), 2);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            embeddedRedis.start();

            Map<SchemaTableName, RedisTableDescription> tableDescriptions = createTpchTableDescriptions(queryRunner.getCoordinator().getMetadata(), tables);

            installRedisPlugin(embeddedRedis, queryRunner, tableDescriptions);

            TestingPrestoClient prestoClient = queryRunner.getClient();

            log.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTable(embeddedRedis, prestoClient, table);
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

    private static void loadTpchTable(EmbeddedRedis embeddedRedis, TestingPrestoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        TestUtils.loadTpchTable(embeddedRedis, prestoClient, redisTableName(table), new QualifiedTableName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)));
        log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String redisTableName(TpchTable<?> table)
    {
        return TPCH_SCHEMA + ":" + table.getTableName().toLowerCase(ENGLISH);
    }

    private static Map<SchemaTableName, RedisTableDescription> createTpchTableDescriptions(Metadata metadata, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        JsonCodec<RedisTableDescription> tableDescriptionJsonCodec = new CodecSupplier<>(RedisTableDescription.class, metadata).get();

        ImmutableMap.Builder<SchemaTableName, RedisTableDescription> tableDescriptions = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            SchemaTableName tpchTable = new SchemaTableName(TPCH_SCHEMA, tableName);

            tableDescriptions.put(loadTpchTableDescription(tableDescriptionJsonCodec, tpchTable.toString(), tpchTable));
        }
        return tableDescriptions.build();
    }

    public static Session createSession()
    {
        return Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("redis")
                .setSchema(TPCH_SCHEMA)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();
    }
}
