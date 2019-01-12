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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

public class TestRedisConnectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(RedisConnectorConfig.class)
                .setNodes("")
                .setDefaultSchema("default")
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/redis/"))
                .setKeyPrefixSchemaTable(false)
                .setRedisKeyDelimiter(":")
                .setRedisConnectTimeout("2000ms")
                .setRedisDataBaseIndex(0)
                .setRedisPassword(null)
                .setRedisScanCount(100)
                .setHideInternalColumns(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("redis.table-description-dir", "/var/lib/redis")
                .put("redis.table-names", "table1, table2, table3")
                .put("redis.default-schema", "redis")
                .put("redis.nodes", "localhost:12345,localhost:23456")
                .put("redis.key-delimiter", ",")
                .put("redis.key-prefix-schema-table", "true")
                .put("redis.scan-count", "20")
                .put("redis.hide-internal-columns", "false")
                .put("redis.connect-timeout", "10s")
                .put("redis.database-index", "5")
                .put("redis.password", "secret")
                .build();

        RedisConnectorConfig expected = new RedisConnectorConfig()
                .setTableDescriptionDir(new File("/var/lib/redis"))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("redis")
                .setNodes("localhost:12345, localhost:23456")
                .setHideInternalColumns(false)
                .setRedisScanCount(20)
                .setRedisConnectTimeout("10s")
                .setRedisDataBaseIndex(5)
                .setRedisPassword("secret")
                .setRedisKeyDelimiter(",")
                .setKeyPrefixSchemaTable(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
