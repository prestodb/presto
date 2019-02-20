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
package com.facebook.presto.password.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class JdbcConfigTest
{
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDefaultConnectionTypeNull()
    {
        assertRecordedDefaults(recordDefaults(JdbcConfig.class)
                .setJdbcAuthConnectionType(null)
                .setJdbcAuthUrl(null)
                .setJdbcAuthSchema(null)
                .setJdbcAuthTable(null)
                .setJdbcAuthPassword(null)
                .setJdbcAuthUser(null)
                .setJdbcAuthCacheTtl((new Duration(1, TimeUnit.HOURS))));
    }

    @Test
    public void testExplicitConfig()
    {
        String connectionURL = "jdbs://localhost:636";
        String user = "user";
        String pass = "pass";
        String schema = "public";
        String table = "users";
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("jdbc.auth.type", JdbcConfig.ConnectionType.postgresql.name())
                .put("jdbc.auth.url", connectionURL)
                .put("jdbc.auth.schema", schema)
                .put("jdbc.auth.table", table)
                .put("jdbc.auth.user", user)
                .put("jdbc.auth.password", pass)
                .put("jdbc.max.open.statements", "200")

                .put("jdbc.pool.initial.size", "10")
                .put("jdbc.pool.min.idle", "10")
                .put("jdbc.pool.max.idle", "20")

                .put("jdbc.auth.cache-ttl", "2m")
                .build();

        JdbcConfig expected = new JdbcConfig()
                .setJdbcAuthConnectionType(JdbcConfig.ConnectionType.postgresql.name())
                .setJdbcAuthUrl(connectionURL)
                .setJdbcAuthSchema(schema)
                .setJdbcAuthTable(table)
                .setJdbcAuthUser(user)
                .setJdbcAuthPassword(pass)
                .setMaxOpenPreparedStatements(200)
                .setPoolInitialSize(10)
                .setPoolMinIdle(10)
                .setPoolMaxIdle(20)
                .setJdbcAuthCacheTtl(new Duration(2, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
