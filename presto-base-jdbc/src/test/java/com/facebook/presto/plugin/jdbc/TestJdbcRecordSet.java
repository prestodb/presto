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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test
public class TestJdbcRecordSet
{
    private static final ConnectorSession session = testSessionBuilder().build().toConnectorSession();

    private TestingDatabase database;
    private JdbcClient jdbcClient;
    private JdbcSplit split;
    private Map<String, JdbcColumnHandle> columnHandles;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();
        split = database.getSplit("example", "numbers");
        columnHandles = database.getColumnHandles("example", "numbers");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, session, split, ImmutableList.of(
                new JdbcColumnHandle("test", "text", JDBC_VARCHAR, VARCHAR, true, Optional.empty()),
                new JdbcColumnHandle("test", "text_short", JDBC_VARCHAR, createVarcharType(32), true, Optional.empty()),
                new JdbcColumnHandle("test", "value", JDBC_BIGINT, BIGINT, true, Optional.empty())));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(VARCHAR, createVarcharType(32), BIGINT));

        recordSet = new JdbcRecordSet(jdbcClient, session, split, ImmutableList.of(
                new JdbcColumnHandle("test", "value", JDBC_BIGINT, BIGINT, true, Optional.empty()),
                new JdbcColumnHandle("test", "text", JDBC_VARCHAR, VARCHAR, true, Optional.empty())));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, VARCHAR));

        recordSet = new JdbcRecordSet(jdbcClient, session, split, ImmutableList.of(
                new JdbcColumnHandle("test", "value", JDBC_BIGINT, BIGINT, true, Optional.empty()),
                new JdbcColumnHandle("test", "value", JDBC_BIGINT, BIGINT, true, Optional.empty()),
                new JdbcColumnHandle("test", "text", JDBC_VARCHAR, VARCHAR, true, Optional.empty())));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, BIGINT, VARCHAR));

        recordSet = new JdbcRecordSet(jdbcClient, session, split, ImmutableList.of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, session, split, ImmutableList.of(
                columnHandles.get("text"),
                columnHandles.get("text_short"),
                columnHandles.get("value")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), VARCHAR);
            assertEquals(cursor.getType(1), createVarcharType(32));
            assertEquals(cursor.getType(2), BIGINT);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(2));
                assertEquals(cursor.getSlice(0), cursor.getSlice(1));
                assertFalse(cursor.isNull(0));
                assertFalse(cursor.isNull(1));
                assertFalse(cursor.isNull(2));
            }

            assertEquals(data, ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .build());
        }
    }

    @Test
    public void testCursorMixedOrder()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, session, split, ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), BIGINT);
            assertEquals(cursor.getType(1), BIGINT);
            assertEquals(cursor.getType(2), VARCHAR);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertEquals(cursor.getLong(0), cursor.getLong(1));
                data.put(cursor.getSlice(2).toStringUtf8(), cursor.getLong(0));
            }

            assertEquals(data, ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .build());
        }
    }

    @Test
    public void testIdempotentClose()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, session, split, ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        RecordCursor cursor = recordSet.cursor();
        cursor.close();
        cursor.close();
    }
}
