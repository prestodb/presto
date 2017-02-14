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
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import io.airlift.log.Logging;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertContains;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestPrestoDatabaseMetaData
{
    private TestingPrestoServer server;

    private Connection connection;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
    {
        server.close();
    }

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setup()
            throws Exception
    {
        connection = createConnection();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws SQLException
    {
        connection.close();
    }

    @Test
    public void testPassEscapeInMetaDataQuery()
            throws Exception
    {
        DatabaseMetaData metaData = connection.getMetaData();

        Set<String> queries = captureQueries(() -> {
            String schemaPattern = "defau" + metaData.getSearchStringEscape() + "_t";
            try (ResultSet resultSet = metaData.getColumns("blackhole", schemaPattern, null, null)) {
                assertFalse(resultSet.next(), "There should be no results");
            }
            return null;
        });

        assertEquals(queries.size(), 1, "Expected exactly one query, got " + queries.size());
        String query = getOnlyElement(queries);

        assertContains(query, "_t' ESCAPE '", "Metadata query does not contain ESCAPE");
    }

    private Set<String> captureQueries(Callable<?> action)
            throws Exception
    {
        Set<QueryId> queryIdsBefore = server.getQueryManager().getAllQueryInfo().stream()
                .map(QueryInfo::getQueryId)
                .collect(toImmutableSet());

        action.call();

        return server.getQueryManager().getAllQueryInfo().stream()
                .filter(queryInfo -> !queryIdsBefore.contains(queryInfo.getQueryId()))
                .map(QueryInfo::getQuery)
                .collect(toImmutableSet());
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }
}
