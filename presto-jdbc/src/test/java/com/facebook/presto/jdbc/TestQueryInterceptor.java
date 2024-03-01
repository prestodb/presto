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
import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.jdbc.TestPrestoDriver.closeQuietly;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestQueryInterceptor
{
    private TestingPrestoServer server;
    private QueryManager qm;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
    }

    @BeforeMethod
    public void setupServer()
            throws Exception
    {
        DistributedQueryRunner distributedQueryRunner = createTpchQueryRunner();
        server = distributedQueryRunner.getCoordinator();
        qm = server.getQueryManager();
    }

    @AfterMethod(alwaysRun = true)
    public void teardownServer()
    {
        closeQuietly(server);
    }

    public static DistributedQueryRunner createTpchQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @Test
    public void testBasicPrePreprocess()
                throws Exception
    {
        String extra = "queryInterceptors=" + TestPreProcess1QueryInterceptor.class.getName();
        try (Connection connection = createConnection(extra);
                Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT 123")) {
                assertEquals(qm.getQueries().size(), 1);
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 456);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testMultiplePrePreprocess()
            throws Exception
    {
        String extra = "queryInterceptors=" + TestPreProcess1QueryInterceptor.class.getName() + ";" + TestPreProcess2QueryInterceptor.class.getName();
        try (Connection connection = createConnection(extra);
                Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT 123")) {
                assertEquals(qm.getQueries().size(), 2);
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 789);
                assertFalse(rs.next());
            }
        }
    }

    public static class TestPreProcess1QueryInterceptor
                implements QueryInterceptor
    {
        @Override
        public Optional<PrestoResultSet> preProcess(String sql, Statement interceptedStatement)
        {
            try {
                ResultSet rs = interceptedStatement.executeQuery("SELECT 456");
                return Optional.ofNullable((PrestoResultSet) rs);
            }
            catch (SQLException ex) {
                throw new RuntimeException("Failed at ", ex);
            }
        }
    }

    public static class TestPreProcess2QueryInterceptor
                implements QueryInterceptor
    {
        @Override
        public Optional<PrestoResultSet> preProcess(String sql, Statement interceptedStatement)
        {
            try {
                ResultSet rs = interceptedStatement.executeQuery("SELECT 789");
                return Optional.ofNullable((PrestoResultSet) rs);
            }
            catch (SQLException ex) {
                throw new RuntimeException("Failed at ", ex);
            }
        }
    }

    @Test
    public void testBasicPostPreprocess()
            throws Exception
    {
        String extra = "queryInterceptors=" + TestPostProcess1QueryInterceptor.class.getName();
        try (Connection connection = createConnection(extra)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet ignored = statement.executeQuery("select * from tpch.sf1000.orders")) {
                    assertEquals(qm.getQueries().size(), 2);
                }
            }
        }

        // Add sleep because deletes are sent async
        Thread.sleep(1000);
        List<BasicQueryInfo> queries = qm.getQueries();
        assertEquals(queries.stream().filter(query -> query.getState() == QueryState.FAILED).count(), 2);
    }

    @Test
    public void testMultiplePostPreprocess()
            throws Exception
    {
        String extra = "queryInterceptors=" + TestPostProcess1QueryInterceptor.class.getName() + ";" + TestPostProcess1QueryInterceptor.class.getName();
        try (Connection connection = createConnection(extra)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet ignored = statement.executeQuery("select * from tpch.sf1000.orders")) {
                    assertEquals(qm.getQueries().size(), 3);
                }
            }
        }

        // Add sleep because deletes are sent async
        Thread.sleep(1000);
        List<BasicQueryInfo> queries = qm.getQueries();
        assertEquals(queries.stream().filter(query -> query.getState() == QueryState.FAILED).count(), 3);
    }

    public static class TestPostProcess1QueryInterceptor
                implements QueryInterceptor
    {
        @Override
        public Optional<PrestoResultSet> postProcess(String sql, Statement interceptedStatement, PrestoResultSet originalResultSet)
        {
            try {
                ResultSet rs = interceptedStatement.executeQuery("select * from tpch.sf1001.orders");
                return Optional.ofNullable((PrestoResultSet) rs);
            }
            catch (SQLException ex) {
                throw new RuntimeException("Failed at ", ex);
            }
        }
    }

    @Test
    public void testPreAndPostProcess()
            throws Exception
    {
        String extra = "queryInterceptors=" + TestPrePostQueryInterceptor.class.getName();
        try (Connection connection = createConnection(extra)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet ignored = statement.executeQuery("SELECT 123")) {
                    List<BasicQueryInfo> queries = qm.getQueries();

                    assertEquals(queries.size(), 2);
                    assertEquals(queries.stream().filter(query -> query.getQuery().contains("456")).count(), 1);
                    assertEquals(queries.stream().filter(query -> query.getQuery().contains("789")).count(), 1);
                }
            }
        }
    }

    public static class TestPrePostQueryInterceptor
            implements QueryInterceptor
    {
        @Override
        public Optional<PrestoResultSet> preProcess(String sql, Statement interceptedStatement)
        {
            try {
                ResultSet rs = interceptedStatement.executeQuery("SELECT 456");
                return Optional.ofNullable((PrestoResultSet) rs);
            }
            catch (SQLException ex) {
                throw new RuntimeException("Failed at ", ex);
            }
        }

        @Override
        public Optional<PrestoResultSet> postProcess(String sql, Statement interceptedStatement, PrestoResultSet originalResultSet)
        {
            try {
                ResultSet rs = interceptedStatement.executeQuery("SELECT 789");
                return Optional.ofNullable((PrestoResultSet) rs);
            }
            catch (SQLException ex) {
                throw new RuntimeException("Failed at ", ex);
            }
        }
    }

    private Connection createConnection(String extra)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/?%s", server.getAddress(), extra);
        return DriverManager.getConnection(url, "test", null);
    }
}
