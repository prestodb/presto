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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public final class MySqlQueryRunner
{
    private MySqlQueryRunner()
    {
    }

    private static final String TPCH_SCHEMA = "tpch";

    public static QueryRunner createMySqlQueryRunner(String jdbcUrl, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createMySqlQueryRunner(jdbcUrl, connectorProperties, tables, "testuser", "testpass");
    }

    public static QueryRunner createMySqlQueryRunner(String jdbcUrl, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables, String username, String password)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 3);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            String jdbcUrlWithoutDatabase = removeDatabaseFromJdbcUrl(jdbcUrl);
            String jdbcUrlWithCredentials = jdbcUrlWithoutDatabase + (jdbcUrlWithoutDatabase.contains("?") ? "&" : "?") +
                    "user=" + username + "&password=" + password;

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", jdbcUrlWithCredentials);
            connectorProperties.putIfAbsent("allow-drop-table", "true");

            queryRunner.installPlugin(new MySqlPlugin());
            queryRunner.createCatalog("mysql", "mysql", connectorProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("mysql")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static String removeDatabaseFromJdbcUrl(String jdbcUrl)
    {
        return jdbcUrl.replaceFirst("/[^/?]+([?]|$)", "/$1");
    }
}
