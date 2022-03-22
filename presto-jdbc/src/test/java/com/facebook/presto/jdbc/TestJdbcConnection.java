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
package com.facebook.presto.jdbc;

import com.facebook.airlift.log.Logging;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.jdbc.TestPrestoDriver.closeQuietly;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.SystemTable.Distribution.ALL_NODES;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestJdbcConnection
{
    private TestingPrestoServer server;

    @DataProvider(name = "customHeaderWithSpecialCharacter")
    public static Object[][] customHeaderWithSpecialCharacter()
    {
        return new Object[][] {{"test.com:1234"}, {"test@test.com"}};
    }

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        Module systemTables = binder -> newSetBinder(binder, SystemTable.class)
                .addBinding().to(ExtraCredentialsSystemTable.class).in(Scopes.SINGLETON);
        server = new TestingPrestoServer(ImmutableList.of(systemTables));

        server.installPlugin(new HiveHadoop2Plugin());
        server.createCatalog("hive", "hive-hadoop2", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", server.getBaseDataDir().resolve("hive").toFile().toURI().toString())
                .put("hive.security", "sql-standard")
                .build());

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET ROLE admin");
            statement.execute("CREATE SCHEMA default");
            statement.execute("CREATE SCHEMA fruit");
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardownServer()
    {
        closeQuietly(server);
    }

    @Test
    public void testCommit()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_commit (x bigint)");
            }

            try (Connection otherConnection = createConnection()) {
                assertThat(listTables(otherConnection)).doesNotContain("test_commit");
            }

            connection.commit();
        }

        try (Connection connection = createConnection()) {
            assertThat(listTables(connection)).contains("test_commit");
        }
    }

    @Test
    public void testRollback()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_rollback (x bigint)");
            }

            try (Connection otherConnection = createConnection()) {
                assertThat(listTables(otherConnection)).doesNotContain("test_rollback");
            }

            connection.rollback();
        }

        try (Connection connection = createConnection()) {
            assertThat(listTables(connection)).doesNotContain("test_rollback");
        }
    }

    @Test
    public void testUse()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            assertThat(connection.getCatalog()).isEqualTo("hive");
            assertThat(connection.getSchema()).isEqualTo("default");

            // change schema
            try (Statement statement = connection.createStatement()) {
                statement.execute("USE fruit");
            }

            assertThat(connection.getCatalog()).isEqualTo("hive");
            assertThat(connection.getSchema()).isEqualTo("fruit");

            // change catalog and schema
            try (Statement statement = connection.createStatement()) {
                statement.execute("USE system.runtime");
            }

            assertThat(connection.getCatalog()).isEqualTo("system");
            assertThat(connection.getSchema()).isEqualTo("runtime");

            // run multiple queries
            assertThat(listTables(connection)).contains("nodes");
            assertThat(listTables(connection)).contains("queries");
            assertThat(listTables(connection)).contains("tasks");
        }
    }

    @Test
    public void testSession()
            throws SQLException
    {
        try (Connection connection = createConnection("sessionProperties=query_max_run_time:2d;max_failed_task_percentage:0.6")) {
            assertThat(listSession(connection))
                    .contains("join_distribution_type|PARTITIONED|PARTITIONED")
                    .contains("exchange_compression|false|false")
                    .contains("query_max_run_time|2d|100.00d")
                    .contains("max_failed_task_percentage|0.6|0.3");

            try (Statement statement = connection.createStatement()) {
                statement.execute("SET SESSION join_distribution_type = 'BROADCAST'");
            }

            assertThat(listSession(connection))
                    .contains("join_distribution_type|BROADCAST|PARTITIONED")
                    .contains("exchange_compression|false|false");

            try (Statement statement = connection.createStatement()) {
                statement.execute("SET SESSION exchange_compression = true");
            }

            assertThat(listSession(connection))
                    .contains("join_distribution_type|BROADCAST|PARTITIONED")
                    .contains("exchange_compression|true|false");
        }
    }

    @Test
    public void testApplicationName()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            assertConnectionSource(connection, "presto-jdbc");
        }

        try (Connection connection = createConnection()) {
            connection.setClientInfo("ApplicationName", "testing");
            assertConnectionSource(connection, "testing");
        }

        try (Connection connection = createConnection("applicationNamePrefix=fruit:")) {
            assertConnectionSource(connection, "fruit:");
        }

        try (Connection connection = createConnection("applicationNamePrefix=fruit:")) {
            connection.setClientInfo("ApplicationName", "testing");
            assertConnectionSource(connection, "fruit:testing");
        }
    }

    @Test
    public void testHttpProtocols()
            throws SQLException
    {
        String extra = "protocols=http11";
        try (Connection connection = createConnection(extra)) {
            assertThat(connection.getCatalog()).isEqualTo("hive");
        }

        // deduplication
        extra = "protocols=http11,http11";
        try (Connection connection = createConnection(extra)) {
            assertThat(connection.getCatalog()).isEqualTo("hive");
        }
    }

    @Test
    public void testExtraCredentials()
            throws SQLException
    {
        Map<String, String> credentials = ImmutableMap.of("test.token.foo", "bar", "test.token.abc", "xyz");
        Connection connection = createConnection("extraCredentials=test.token.foo:bar;test.token.abc:xyz");

        assertTrue(connection instanceof PrestoConnection);
        PrestoConnection prestoConnection = connection.unwrap(PrestoConnection.class);
        assertEquals(prestoConnection.getExtraCredentials(), credentials);
        assertEquals(listExtraCredentials(connection), credentials);
    }

    @Test
    public void testCustomHeaders()
            throws SQLException, UnsupportedEncodingException
    {
        Map<String, String> customHeadersMap = ImmutableMap.of("testHeaderKey", "testHeaderValue");
        String customHeaders = "testHeaderKey:testHeaderValue";
        String encodedCustomHeaders = URLEncoder.encode(customHeaders, StandardCharsets.UTF_8.toString());
        Connection connection = createConnection("customHeaders=" + encodedCustomHeaders);
        assertTrue(connection instanceof PrestoConnection);
        PrestoConnection prestoConnection = connection.unwrap(PrestoConnection.class);
        assertEquals(prestoConnection.getCustomHeaders(), customHeadersMap);
    }

    @Test(dataProvider = "customHeaderWithSpecialCharacter")
    public void testCustomHeadersWithSpecialCharacters(String testHeaderValue)
            throws SQLException, UnsupportedEncodingException
    {
        Map<String, String> customHeadersMap = ImmutableMap.of("testHeaderKey", URLEncoder.encode(testHeaderValue, StandardCharsets.UTF_8.toString()));
        String customHeaders = "testHeaderKey:" + URLEncoder.encode(testHeaderValue, StandardCharsets.UTF_8.toString()) + "";
        String encodedCustomHeaders = URLEncoder.encode(customHeaders, StandardCharsets.UTF_8.toString());
        Connection connection = createConnection("customHeaders=" + encodedCustomHeaders);
        assertTrue(connection instanceof PrestoConnection);
        PrestoConnection prestoConnection = connection.unwrap(PrestoConnection.class);
        assertEquals(prestoConnection.getCustomHeaders(), customHeadersMap);
    }

    @Test
    public void testQueryInterceptors()
            throws SQLException
    {
        String extra = "queryInterceptors=" + TestNoopQueryInterceptor.class.getName();
        try (PrestoConnection connection = createConnection(extra).unwrap(PrestoConnection.class)) {
            List<QueryInterceptor> queryInterceptorInstances = connection.getQueryInterceptorInstances();
            assertEquals(queryInterceptorInstances.size(), 1);
            assertEquals(queryInterceptorInstances.get(0).getClass().getName(), TestNoopQueryInterceptor.class.getName());
        }
    }

    @Test
    public void testConnectionProperties()
            throws SQLException
    {
        String extra = "extraCredentials=test.token.foo:bar;test.token.abc:xyz";
        try (PrestoConnection connection = createConnection(extra).unwrap(PrestoConnection.class)) {
            Properties connectionProperties = connection.getConnectionProperties();
            assertTrue(connectionProperties.size() > 0);
            assertNotNull(connectionProperties.getProperty("extraCredentials"));
        }
    }

    public static class TestNoopQueryInterceptor
            implements QueryInterceptor
    {
        @Override
        public void init(Map<String, String> properties)
        {
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        return createConnection("");
    }

    private Connection createConnection(String extra)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/hive/default?%s", server.getAddress(), extra);
        return DriverManager.getConnection(url, "admin", null);
    }

    private static Set<String> listTables(Connection connection)
            throws SQLException
    {
        ImmutableSet.Builder<String> set = ImmutableSet.builder();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW TABLES")) {
            while (rs.next()) {
                set.add(rs.getString(1));
            }
        }
        return set.build();
    }

    private static Set<String> listSession(Connection connection)
            throws SQLException
    {
        ImmutableSet.Builder<String> set = ImmutableSet.builder();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW SESSION")) {
            while (rs.next()) {
                set.add(Joiner.on('|').join(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3)));
            }
        }
        return set.build();
    }

    private static Map<String, String> listExtraCredentials(Connection connection)
            throws SQLException
    {
        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM system.test.extra_credentials");
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        while (rs.next()) {
            builder.put(rs.getString("name"), rs.getString("value"));
        }
        return builder.build();
    }

    private static void assertConnectionSource(Connection connection, String expectedSource)
            throws SQLException
    {
        String queryId;
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT 123")) {
            queryId = rs.unwrap(PrestoResultSet.class).getQueryId();
        }

        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT source FROM system.runtime.queries WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertThat(rs.getString("source")).isEqualTo(expectedSource);
                assertFalse(rs.next());
            }
        }
    }

    private static class ExtraCredentialsSystemTable
            implements SystemTable
    {
        private static final SchemaTableName NAME = new SchemaTableName("test", "extra_credentials");

        public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
                .column("name", createUnboundedVarcharType())
                .column("value", createUnboundedVarcharType())
                .build();

        @Override
        public Distribution getDistribution()
        {
            return ALL_NODES;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata()
        {
            return METADATA;
        }

        @Override
        public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
        {
            InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(METADATA);
            session.getIdentity().getExtraCredentials().forEach(table::addRow);
            return table.build().cursor();
        }
    }
}
