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
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.facebook.airlift.testing.Assertions.assertContains;
import static com.facebook.presto.common.type.VarcharType.MAX_LENGTH;
import static com.facebook.presto.jdbc.TestPrestoDriver.closeQuietly;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
            throws Exception
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
    {
        closeQuietly(connection);
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

    @Test
    public void testGetTypeInfo()
            throws Exception
    {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet typeInfo = metaData.getTypeInfo();
        while (typeInfo.next()) {
            int jdbcType = typeInfo.getInt("DATA_TYPE");
            switch (jdbcType) {
                case Types.BIGINT:
                    assertColumnSpec(typeInfo, Types.BIGINT, 19L, 10L, "bigint");
                    break;
                case Types.BOOLEAN:
                    assertColumnSpec(typeInfo, Types.BOOLEAN, null, null, "boolean");
                    break;
                case Types.INTEGER:
                    assertColumnSpec(typeInfo, Types.INTEGER, 10L, 10L, "integer");
                    break;
                case Types.DECIMAL:
                    assertColumnSpec(typeInfo, Types.DECIMAL, 38L, 10L, "decimal");
                    break;
                case Types.VARCHAR:
                    assertColumnSpec(typeInfo, Types.VARCHAR, null, null, "varchar");
                    break;
                case Types.TIMESTAMP:
                    assertColumnSpec(typeInfo, Types.TIMESTAMP, 23L, null, "timestamp");
                    break;
                case Types.DOUBLE:
                    assertColumnSpec(typeInfo, Types.DOUBLE, 53L, 2L, "double");
                    break;
            }
        }
    }

    @Test
    public void testGetClientInfoProperties()
            throws Exception
    {
        try (ResultSet resultSet = connection.getMetaData().getClientInfoProperties()) {
            ResultSetMetaData metadata = resultSet.getMetaData();
            assertEquals(metadata.getColumnCount(), 4);
            assertEquals(metadata.getColumnName(1), "NAME");
            assertEquals(metadata.getColumnName(2), "MAX_LEN");
            assertEquals(metadata.getColumnName(3), "DEFAULT_VALUE");
            assertEquals(metadata.getColumnName(4), "DESCRIPTION");

            assertTrue(resultSet.next());
            assertEquals(resultSet.getString(1), "ApplicationName");
            assertEquals(resultSet.getInt(2), MAX_LENGTH);
            assertEquals(resultSet.getString(3), "presto-jdbc");
            assertEquals(resultSet.getString(4), "Sets the source of the session");

            assertTrue(resultSet.next());
            assertEquals(resultSet.getString(1), "ClientInfo");
            assertEquals(resultSet.getInt(2), MAX_LENGTH);
            assertNull(resultSet.getString(3));
            assertEquals(resultSet.getString(4), "Sets the client info of the session");

            assertTrue(resultSet.next());
            assertEquals(resultSet.getString(1), "ClientTags");
            assertEquals(resultSet.getInt(2), MAX_LENGTH);
            assertNull(resultSet.getString(3));
            assertEquals(resultSet.getString(4), "Comma-delimited string of tags for the session");

            assertTrue(resultSet.next());
            assertEquals(resultSet.getString(1), "TraceToken");
            assertEquals(resultSet.getInt(2), MAX_LENGTH);
            assertNull(resultSet.getString(3));
            assertEquals(resultSet.getString(4), "Sets the trace token of the session");

            assertFalse(resultSet.next());
        }
    }

    private static void assertColumnSpec(ResultSet rs, int dataType, Long precision, Long numPrecRadix, String typeName)
            throws SQLException
    {
        String message = " of " + typeName + ": ";
        assertEquals(rs.getObject("TYPE_NAME"), typeName, "TYPE_NAME" + message);
        assertEquals(rs.getObject("DATA_TYPE"), (long) dataType, "DATA_TYPE" + message);
        assertEquals(rs.getObject("PRECISION"), precision, "PRECISION" + message);
        assertEquals(rs.getObject("LITERAL_PREFIX"), null, "LITERAL_PREFIX" + message);
        assertEquals(rs.getObject("LITERAL_SUFFIX"), null, "LITERAL_SUFFIX" + message);
        assertEquals(rs.getObject("CREATE_PARAMS"), null, "CREATE_PARAMS" + message);
        assertEquals(rs.getObject("NULLABLE"), (long) DatabaseMetaData.typeNullable, "NULLABLE" + message);
        assertEquals(rs.getObject("CASE_SENSITIVE"), false, "CASE_SENSITIVE" + message);
        assertEquals(rs.getObject("SEARCHABLE"), (long) DatabaseMetaData.typeSearchable, "SEARCHABLE" + message);
        assertEquals(rs.getObject("UNSIGNED_ATTRIBUTE"), null, "UNSIGNED_ATTRIBUTE" + message);
        assertEquals(rs.getObject("FIXED_PREC_SCALE"), false, "FIXED_PREC_SCALE" + message);
        assertEquals(rs.getObject("AUTO_INCREMENT"), null, "AUTO_INCREMENT" + message);
        assertEquals(rs.getObject("LOCAL_TYPE_NAME"), null, "LOCAL_TYPE_NAME" + message);
        assertEquals(rs.getObject("MINIMUM_SCALE"), 0L, "MINIMUM_SCALE" + message);
        assertEquals(rs.getObject("MAXIMUM_SCALE"), 0L, "MAXIMUM_SCALE" + message);
        assertEquals(rs.getObject("SQL_DATA_TYPE"), null, "SQL_DATA_TYPE" + message);
        assertEquals(rs.getObject("SQL_DATETIME_SUB"), null, "SQL_DATETIME_SUB" + message);
        assertEquals(rs.getObject("NUM_PREC_RADIX"), numPrecRadix, "NUM_PREC_RADIX" + message);
    }

    private Set<String> captureQueries(Callable<?> action)
            throws Exception
    {
        Set<QueryId> queryIdsBefore = server.getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQueryId)
                .collect(toImmutableSet());

        action.call();

        return server.getQueryManager().getQueries().stream()
                .filter(queryInfo -> !queryIdsBefore.contains(queryInfo.getQueryId()))
                .map(BasicQueryInfo::getQuery)
                .collect(toImmutableSet());
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }
}
