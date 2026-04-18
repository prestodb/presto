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
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

/**
 * Query runner for SQL Server integration tests.
 */
public final class SqlServerQueryRunner
{
    private SqlServerQueryRunner() {}

    public static QueryRunner createSqlServerQueryRunner(
            MSSQLServerContainer sqlServerContainer,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createSqlServerQueryRunner(sqlServerContainer, extraProperties, ImmutableList.copyOf(tables));
    }

    public static QueryRunner createSqlServerQueryRunner(
            MSSQLServerContainer sqlServerContainer,
            Map<String, String> extraProperties,
            List<TpchTable<?>> tables)
            throws Exception
    {
        QueryRunner queryRunner = null;
        try {
            queryRunner = new DistributedQueryRunner(createSession(), 3, extraProperties);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            // Create test database in SQL Server
            // Note: 'dbo' is the default schema and is automatically created with the database
            try (Connection connection = DriverManager.getConnection(
                    sqlServerContainer.getJdbcUrl(),
                    sqlServerContainer.getUsername(),
                    sqlServerContainer.getPassword());
                    Statement statement = connection.createStatement()) {
                statement.execute("CREATE DATABASE tpch");
            }

            Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                    .put("connection-url", sqlServerContainer.getJdbcUrl() + ";databaseName=tpch")
                    .put("connection-user", sqlServerContainer.getUsername())
                    .put("connection-password", sqlServerContainer.getPassword())
                    .build();

            queryRunner.installPlugin(new SqlServerPlugin());
            queryRunner.createCatalog("sqlserver", "sqlserver", connectorProperties);

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
                .setCatalog("sqlserver")
                .setSchema("dbo")
                .build();
    }

    private static void closeAllSuppress(Throwable rootCause, AutoCloseable... closeables)
    {
        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            }
            catch (Exception e) {
                // Self-suppression not permitted
                if (rootCause != e) {
                    rootCause.addSuppressed(e);
                }
            }
        }
    }
}
