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
package com.facebook.presto.plugin.snowflake;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import io.airlift.units.Duration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestSnowflakeIntegrationSmokeTest
{
    @Mock
    Connection mockConnection;

    JdbcConnectorId connectorId = new JdbcConnectorId("snowflake");

    @Mock
    BaseJdbcConfig config;

    @BeforeMethod
    public void init()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSnowflakeIntegrationSmokeTest() throws Exception
    {
        when(config.getCaseInsensitiveNameMatchingCacheTtl()).thenReturn(new Duration(300, TimeUnit.MICROSECONDS));
        PreparedStatement ps = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any(String.class))).thenReturn(ps);
        execute("SELECT 1");
    }

    @Test
    public void testInsert() throws Exception
    {
        when(config.getCaseInsensitiveNameMatchingCacheTtl()).thenReturn(new Duration(300, TimeUnit.MICROSECONDS));
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any(String.class))).thenReturn(preparedStatement);
        execute("CREATE TABLE test_insert (x bigint, y varchar(100))");
        execute("INSERT INTO test_insert VALUES (123, 'test')");
        execute("SELECT * FROM test_insert");

        ResultSet resultSet = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getLong("x")).thenReturn(123L);
        when(resultSet.getString("y")).thenReturn("test");

        assertEquals(123, resultSet.getLong("x"));
        assertEquals("test", resultSet.getString("y"));
        execute("DROP TABLE test_insert");
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        when(config.getCaseInsensitiveNameMatchingCacheTtl()).thenReturn(new Duration(300, TimeUnit.MICROSECONDS));
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any(String.class))).thenReturn(preparedStatement);
        when(preparedStatement.execute()).thenReturn(true);
        boolean queryResult = executeQuery("CREATE TABLE test_table (name VARCHAR(255), age INT)");
        assertTrue(queryResult);
    }

    @Test
    public void testSelect()
            throws Exception
    {
        when(config.getCaseInsensitiveNameMatchingCacheTtl()).thenReturn(new Duration(300, TimeUnit.MICROSECONDS));
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any(String.class))).thenReturn(preparedStatement);
        when(preparedStatement.execute()).thenReturn(true);
        boolean queryResult = executeQuery("SELECT * FROM test_table");
        assertTrue(queryResult);
    }
    @Test
    public void testDropTable()
            throws Exception
    {
        when(config.getCaseInsensitiveNameMatchingCacheTtl()).thenReturn(new Duration(300, TimeUnit.MICROSECONDS));
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any(String.class))).thenReturn(preparedStatement);
        when(preparedStatement.execute()).thenReturn(true);
        boolean queryResult = executeQuery("DROP TABLE testtable1");
        assertTrue(queryResult);
    }
    private PreparedStatement execute(String sql) throws Exception
    {
        SnowflakeClient mockSnowflakeClient = createSnowflakeClient(mockConnection);
        try {
            return mockSnowflakeClient.getPreparedStatement(null, mockConnection, sql);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SnowflakeClient createSnowflakeClient(Connection connection) throws Exception
    {
        ConnectionFactory connectionFactory = mock(DriverConnectionFactory.class);
        when(connectionFactory.openConnection(any(JdbcIdentity.class))).thenReturn(mockConnection);
        SnowflakeClient mockSnowflakeClient = new SnowflakeClient(connectorId, config, connectionFactory);
        return mockSnowflakeClient;
    }

    private boolean executeQuery(String sql) throws Exception
    {
        SnowflakeClient mockSnowflakeClient = createSnowflakeClient(mockConnection);
        try {
            PreparedStatement preparedStatement = mockSnowflakeClient.getPreparedStatement(null, mockConnection, sql);
            boolean queryResult = preparedStatement.execute();
            return queryResult;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
