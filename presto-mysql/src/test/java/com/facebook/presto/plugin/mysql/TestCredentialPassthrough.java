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
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.mysql.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.removeDatabaseFromJdbcUrl;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestCredentialPassthrough
{
    private static final String TEST_SCHEMA = "test_database";
    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "testpass";

    private final MySQLContainer mysqlContainer;
    private final QueryRunner mySqlQueryRunner;

    public TestCredentialPassthrough()
            throws Exception
    {
        mysqlContainer = new MySQLContainer("mysql:8.0")
                .withDatabaseName(TEST_SCHEMA)
                .withUsername(TEST_USER)
                .withPassword(TEST_PASSWORD);
        mysqlContainer.start();

        try (Connection connection = DriverManager.getConnection(mysqlContainer.getJdbcUrl(), TEST_USER, TEST_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + TEST_SCHEMA);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create " + TEST_SCHEMA, e);
        }

        mySqlQueryRunner = createQueryRunner(mysqlContainer);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (mysqlContainer != null) {
            mysqlContainer.stop();
        }
    }

    @Test
    public void testCredentialPassthrough()
    {
        mySqlQueryRunner.execute(getSession(mysqlContainer), "CREATE TABLE test_create (a bigint, b double, c varchar)");
    }

    public static QueryRunner createQueryRunner(MySQLContainer mysqlContainer)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
            queryRunner.installPlugin(new MySqlPlugin());
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", getConnectionUrl(mysqlContainer))
                    .put("user-credential-name", "mysql.user")
                    .put("password-credential-name", "mysql.password")
                    .build();
            queryRunner.createCatalog("mysql", "mysql", properties);

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session getSession(MySQLContainer mysqlContainer)
    {
        Map<String, String> extraCredentials = ImmutableMap.of("mysql.user", mysqlContainer.getUsername(), "mysql.password", mysqlContainer.getPassword());
        return testSessionBuilder()
                .setCatalog("mysql")
                .setSchema(TEST_SCHEMA)
                .setIdentity(new Identity(
                        mysqlContainer.getUsername(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        extraCredentials,
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .build();
    }

    private static String getConnectionUrl(MySQLContainer mysqlContainer)
    {
        String jdbcUrlWithoutDatabase = removeDatabaseFromJdbcUrl(mysqlContainer.getJdbcUrl());
        return format("%s?useSSL=false&allowPublicKeyRetrieval=true", jdbcUrlWithoutDatabase.split("\\?")[0]);
    }
}
