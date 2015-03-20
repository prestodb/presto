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
package com.facebook.presto.redis.util;

import com.facebook.presto.redis.RedisPlugin;
import com.facebook.presto.redis.RedisQueryRunner;
import com.facebook.presto.redis.RedisTableDescription;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.base.Joiner;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import static java.lang.String.format;

public final class TestUtils
{
    private TestUtils() {}

    public static void installRedisPlugin(EmbeddedRedis embeddedRedis, QueryRunner queryRunner, Map<SchemaTableName, RedisTableDescription> tableDescriptions)
    {
        RedisPlugin redisPlugin = new RedisPlugin();
        redisPlugin.setTableDescriptionSupplier(Suppliers.ofInstance(tableDescriptions));
        queryRunner.installPlugin(redisPlugin);

        Map<String, String> redisConfig = ImmutableMap.of(
                "redis.nodes", embeddedRedis.getConnectString() + ":" + embeddedRedis.getPort(),
                "redis.table-names", Joiner.on(",").join(tableDescriptions.keySet()),
                "redis.default-schema", "default",
                "redis.hide-internal-columns", "true",
                "redis.key-prefix-schema-table", "true");
        queryRunner.createCatalog("redis", "redis", redisConfig);
    }

    public static void loadTpchTable(EmbeddedRedis embeddedRedis, TestingPrestoClient prestoClient, String tableName, QualifiedTableName tpchTableName)
    {
        RedisLoader tpchLoader = new RedisLoader(embeddedRedis.getJedisPool(), tableName, prestoClient.getServer(), prestoClient.getDefaultSession());
        tpchLoader.execute(format("SELECT * from %s", tpchTableName));
    }

    public static Map.Entry<SchemaTableName, RedisTableDescription> loadTpchTableDescription(JsonCodec<RedisTableDescription> tableDescriptionJsonCodec, String tableName, SchemaTableName schemaTableName)
            throws IOException
    {
        RedisTableDescription tpchTemplate = tableDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(TestUtils.class.getResourceAsStream(format("/tpch/" + RedisQueryRunner.dataFormat + "/%s.json", schemaTableName.getTableName()))));

        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new RedisTableDescription(schemaTableName.getTableName(), schemaTableName.getSchemaName(), tpchTemplate.getKey(), tpchTemplate.getValue()));
    }

    public static Map.Entry<SchemaTableName, RedisTableDescription> createEmptyTableDescription(SchemaTableName schemaTableName)
    {
        return new AbstractMap.SimpleImmutableEntry<>(
                schemaTableName,
                new RedisTableDescription(schemaTableName.getTableName(), schemaTableName.getSchemaName(), null, null));
    }
}
