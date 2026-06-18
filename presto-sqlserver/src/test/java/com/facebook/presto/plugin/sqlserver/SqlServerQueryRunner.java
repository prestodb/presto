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
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public final class SqlServerQueryRunner
{
    private SqlServerQueryRunner()
    {
    }

    private static final String TPCH_SCHEMA = "tpch";

    public static QueryRunner createSqlServerQueryRunner(String jdbcUrl, Map<String, String> connectorProperties, String username, String password)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 3);

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", jdbcUrl);
            connectorProperties.putIfAbsent("connection-user", username);
            connectorProperties.putIfAbsent("connection-password", password);

            createSchema(jdbcUrl, "tpch", username, password);

            queryRunner.installPlugin(new SqlServerPlugin());
            queryRunner.createCatalog("sqlserver", "sqlserver", connectorProperties);
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
                .setCatalog("sqlserver")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    private static void createSchema(String url, String schema, String username, String password)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(url, username, password);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
    }
}
