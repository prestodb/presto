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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.plugin.blackhole.BlackHolePlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.testing.TestingWarningCollector;
import com.facebook.presto.testing.TestingWarningCollectorConfig;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.jdbc.TestPrestoDriver.closeQuietly;
import static com.facebook.presto.jdbc.TestPrestoDriver.waitForNodeRefresh;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestJdbcWarnings
{
    // Number of warnings preloaded to the testing warning collector before a query runs
    private static final int PRELOADED_WARNINGS = 5;

    private TestingPrestoServer server;
    private Connection connection;
    private Statement statement;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        server = new TestingPrestoServer(
                true,
                ImmutableMap.<String, String>builder()
                        .put("testing-warning-collector.add-warnings", "true")
                        .put("testing-warning-collector.preloaded-warnings", String.valueOf(PRELOADED_WARNINGS))
                        .build(),
                null,
                null,
                new SqlParserOptions(),
                ImmutableList.of());
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");
        waitForNodeRefresh(server);
    }

    @AfterClass(alwaysRun = true)
    public void teardownServer()
    {
        closeQuietly(server);
    }

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setup()
            throws Exception
    {
        connection = createConnection();
        statement = connection.createStatement();
    }

    @AfterMethod
    public void teardown()
    {
        closeQuietly(statement);
        closeQuietly(connection);
    }

    @Test
    public void testStatementWarnings()
            throws SQLException
    {
        assertFalse(statement.execute("CREATE SCHEMA blackhole.test_schema"));
        SQLWarning warning = statement.getWarnings();
        assertNotNull(warning);
        TestingWarningCollectorConfig warningCollectorConfig = new TestingWarningCollectorConfig().setPreloadedWarnings(PRELOADED_WARNINGS);
        TestingWarningCollector warningCollector = new TestingWarningCollector(new WarningCollectorConfig(), warningCollectorConfig);
        List<PrestoWarning> expectedWarnings = warningCollector.getWarnings();
        assertStartsWithExpectedWarnings(warning, fromPrestoWarnings(expectedWarnings));
        statement.clearWarnings();
        assertNull(statement.getWarnings());
    }

    @Test
    public void testLongRunningStatement()
            throws SQLException, InterruptedException
    {
        ExecutorService queryExecutor = newSingleThreadExecutor(daemonThreadsNamed("test-%s"));
        QueryCreationFuture queryCreationFuture = new QueryCreationFuture();
        queryExecutor.submit(() -> {
            try {
                statement.execute("CREATE SCHEMA blackhole.blackhole");
                statement.execute("CREATE TABLE blackhole.blackhole.test_table AS SELECT 1 AS col1 FROM tpch.sf1.lineitem CROSS JOIN tpch.sf1.lineitem");
                queryCreationFuture.set(null);
            }
            catch (Throwable e) {
                queryCreationFuture.setException(e);
            }
        });
        while (statement.getWarnings() == null) {
            Thread.sleep(100);
        }
        SQLWarning warning = statement.getWarnings();
        Set<WarningEntry> currentWarnings = new HashSet<>();
        assertTrue(currentWarnings.add(new WarningEntry(warning)));
        for (int warnings = 1; !queryCreationFuture.isDone() && warnings < 100; warnings++) {
            for (SQLWarning nextWarning = warning.getNextWarning(); nextWarning == null; nextWarning = warning.getNextWarning()) {
                // Wait for new warnings
            }
            warning = warning.getNextWarning();
            assertTrue(currentWarnings.add(new WarningEntry(warning)));
            Thread.sleep(100);
        }
        assertEquals(currentWarnings.size(), 100);
        queryExecutor.shutdownNow();
    }

    @Test
    public void testLongRunningQuery()
            throws SQLException, InterruptedException
    {
        ExecutorService queryExecutor = newSingleThreadExecutor(daemonThreadsNamed("test-%s"));
        QueryCreationFuture queryCreationFuture = new QueryCreationFuture();
        queryExecutor.submit(() -> {
            try {
                statement.execute("SELECT 1 AS col1 FROM tpch.sf1.lineitem CROSS JOIN tpch.sf1.lineitem");
                queryCreationFuture.set(null);
            }
            catch (Throwable e) {
                queryCreationFuture.setException(e);
            }
        });
        while (statement.getResultSet() == null) {
            Thread.sleep(100);
        }
        ResultSet resultSet = statement.getResultSet();
        Set<WarningEntry> currentWarnings = new HashSet<>();
        for (int rows = 0; !queryCreationFuture.isDone() && rows < 10; ) {
            if (resultSet.next()) {
                for (SQLWarning warning = resultSet.getWarnings(); warning.getNextWarning() != null; warning = warning.getNextWarning()) {
                    assertTrue(currentWarnings.add(new WarningEntry(warning.getNextWarning())));
                }
            }
            else {
                break;
            }
            Thread.sleep(100);
        }
        queryExecutor.shutdownNow();
    }

    @Test
    public void testExecuteQueryWarnings()
            throws SQLException
    {
        try (ResultSet rs = statement.executeQuery("SELECT a FROM (VALUES 1, 2, 3) t(a)")) {
            assertNull(statement.getConnection().getWarnings());
            assertNull(statement.getWarnings());
            assertNull(rs.getWarnings());
            Set<WarningEntry> currentWarnings = new HashSet<>();
            while (rs.next()) {
                assertWarnings(rs.getWarnings(), currentWarnings);
            }

            TestingWarningCollectorConfig warningCollectorConfig = new TestingWarningCollectorConfig().setPreloadedWarnings(PRELOADED_WARNINGS).setAddWarnings(true);
            TestingWarningCollector warningCollector = new TestingWarningCollector(new WarningCollectorConfig(), warningCollectorConfig);
            List<PrestoWarning> expectedWarnings = warningCollector.getWarnings();
            for (PrestoWarning prestoWarning : expectedWarnings) {
                assertTrue(currentWarnings.contains(new WarningEntry(new PrestoSqlWarning(prestoWarning))));
            }
        }
    }

    @Test
    public void testSqlWarning()
    {
        ImmutableList.Builder<PrestoWarning> builder = ImmutableList.builder();
        for (int i = 0; i < 3; i++) {
            builder.add(new PrestoWarning(new WarningCode(i, "CODE_" + i), "warning message " + i));
        }
        List<PrestoWarning> warnings = builder.build();
        SQLWarning warning = fromPrestoWarnings(warnings);
        assertEquals(Iterators.size(warning.iterator()), warnings.size());
        assertWarningsEqual(warning, new PrestoSqlWarning(warnings.get(0)));
        assertWarningsEqual(warning.getNextWarning(), new PrestoSqlWarning(warnings.get(1)));
        assertWarningsEqual(warning.getNextWarning().getNextWarning(), new PrestoSqlWarning(warnings.get(2)));
    }

    private static SQLWarning fromPrestoWarnings(List<PrestoWarning> warnings)
    {
        requireNonNull(warnings, "warnings is null");
        assertFalse(warnings.isEmpty());
        Iterator<PrestoWarning> iterator = warnings.iterator();
        PrestoSqlWarning first = new PrestoSqlWarning(iterator.next());
        SQLWarning current = first;
        while (iterator.hasNext()) {
            current.setNextWarning(new PrestoSqlWarning(iterator.next()));
            current = current.getNextWarning();
        }
        return first;
    }

    private static void assertWarningsEqual(SQLWarning actual, SQLWarning expected)
    {
        assertEquals(actual.getMessage(), expected.getMessage());
        assertEquals(actual.getSQLState(), expected.getSQLState());
        assertEquals(actual.getErrorCode(), expected.getErrorCode());
    }

    private static void addWarnings(Set<WarningEntry> currentWarnings, SQLWarning newWarning)
    {
        if (newWarning == null) {
            return;
        }
        for (Throwable warning : newWarning) {
            WarningEntry entry = new WarningEntry(warning);
            currentWarnings.add(entry);
        }
    }

    //TODO: this method seems to be copied in multiple test classes in this package, should it be moved to a utility?
    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress(), "blackhole", "blackhole");
        return DriverManager.getConnection(url, "test", null);
    }

    private static void assertWarnings(SQLWarning warning, Set<WarningEntry> currentWarnings)
    {
        assertNotNull(warning);
        int previousSize = currentWarnings.size();
        addWarnings(currentWarnings, warning);
        assertTrue(currentWarnings.size() >= previousSize);
    }

    private static void assertStartsWithExpectedWarnings(SQLWarning warning, SQLWarning expected)
    {
        assertNotNull(expected);
        assertNotNull(warning);
        while (true) {
            assertWarningsEqual(warning, expected);
            warning = warning.getNextWarning();
            expected = expected.getNextWarning();
            if (expected == null) {
                return;
            }
            assertNotNull(warning);
        }
    }

    private static class WarningEntry
    {
        public final int vendorCode;
        public final String sqlState;
        public final String message;

        public WarningEntry(Throwable throwable)
        {
            requireNonNull(throwable, "throwable is null");
            assertTrue(throwable instanceof SQLWarning);
            SQLWarning warning = (SQLWarning) throwable;
            this.vendorCode = warning.getErrorCode();
            this.sqlState = requireNonNull(warning.getSQLState(), "SQLState is null");
            this.message = requireNonNull(warning.getMessage(), "message is null");
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof WarningEntry)) {
                return false;
            }
            WarningEntry that = (WarningEntry) other;
            return vendorCode == that.vendorCode && sqlState.equals(that.sqlState) && message.equals(that.message);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(vendorCode, sqlState, message);
        }
    }

    private static class QueryCreationFuture
            extends AbstractFuture<QueryInfo>
    {
        @Override
        protected boolean set(QueryInfo value)
        {
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // query submission can not be canceled
            return false;
        }
    }
}
