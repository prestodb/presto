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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.redis.RedisPlugin;
import com.facebook.presto.redis.RedisTableDescription;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.Map;

import static java.lang.String.format;

public final class RedisTestUtils
{
    private RedisTestUtils() {}

    public static void installRedisPlugin(EmbeddedRedis embeddedRedis, QueryRunner queryRunner, Map<SchemaTableName, RedisTableDescription> tableDescriptions)
    {
        RedisPlugin redisPlugin = new RedisPlugin();
        redisPlugin.setTableDescriptionSupplier(() -> tableDescriptions);
        queryRunner.installPlugin(redisPlugin);

        Map<String, String> redisConfig = ImmutableMap.of(
                "redis.nodes", embeddedRedis.getConnectString() + ":" + embeddedRedis.getPort(),
                "redis.table-names", Joiner.on(",").join(tableDescriptions.keySet()),
                "redis.default-schema", "default",
                "redis.hide-internal-columns", "true",
                "redis.key-prefix-schema-table", "true");
        queryRunner.createCatalog("redis", "redis", redisConfig);
    }

    public static void loadTpchTable(EmbeddedRedis embeddedRedis, TestingPrestoClient prestoClient, String tableName, QualifiedObjectName tpchTableName, String dataFormat)
    {
        RedisLoader tpchLoader = new RedisLoader(prestoClient.getServer(), prestoClient.getDefaultSession(), embeddedRedis.getJedisPool(), tableName, dataFormat);
        tpchLoader.execute(format("SELECT * from %s", tpchTableName));
    }

    public static Map.Entry<SchemaTableName, RedisTableDescription> loadTpchTableDescription(
            JsonCodec<RedisTableDescription> tableDescriptionJsonCodec,
            SchemaTableName schemaTableName,
            String dataFormat)
            throws IOException
    {
        RedisTableDescription tpchTemplate;
        try (InputStream data = RedisTestUtils.class.getResourceAsStream(format("/tpch/%s/%s.json", dataFormat, schemaTableName.getTableName()))) {
            tpchTemplate = tableDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(data));
        }

        RedisTableDescription tableDescription = new RedisTableDescription(
                schemaTableName.getTableName(),
                schemaTableName.getSchemaName(),
                tpchTemplate.getKey(),
                tpchTemplate.getValue());

        return new AbstractMap.SimpleImmutableEntry<>(schemaTableName, tableDescription);
    }

    public static Map.Entry<SchemaTableName, RedisTableDescription> createEmptyTableDescription(SchemaTableName schemaTableName)
    {
        RedisTableDescription tableDescription = new RedisTableDescription(
                schemaTableName.getTableName(),
                schemaTableName.getSchemaName(),
                null,
                null);

        return new AbstractMap.SimpleImmutableEntry<>(schemaTableName, tableDescription);
    }
}
