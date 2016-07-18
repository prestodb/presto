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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.Iterables;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.FIELD_LENGTH_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.PAGES_PER_SPLIT_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.ROWS_PER_PAGE_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleConnector.SPLIT_COUNT_PROPERTY;
import static com.facebook.presto.plugin.blackhole.BlackHoleQueryRunner.createQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestBlackHoleSmoke
{
    private QueryRunner queryRunner;

    @BeforeTest
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @AfterTest
    public void tearDown()
    {
        assertThatNoBlackHoleTableIsCreated();
        queryRunner.close();
    }

    @Test
    public void createTableWhenTableIsAlreadyCreated()
            throws SQLException
    {
        String createTableSql = "CREATE TABLE nation as SELECT * FROM tpch.tiny.nation";
        queryRunner.execute(createTableSql);
        try {
            queryRunner.execute(createTableSql);
            fail("Expected exception to be thrown here!");
        }
        catch (RuntimeException ex) { // it has to RuntimeException as FailureInfo$FailureException is private
            assertTrue(ex.getMessage().equals("line 1:1: Destination table 'blackhole.default.nation' already exists"));
        }
        finally {
            assertThatQueryReturnsValue("DROP TABLE nation", true);
        }
    }

    @Test
    public void blackHoleConnectorUsage()
            throws SQLException
    {
        assertThatQueryReturnsValue("CREATE TABLE nation as SELECT * FROM tpch.tiny.nation", 25L);

        List<QualifiedObjectName> tableNames = listBlackHoleTables();
        assertTrue(tableNames.size() == 1, "Expected only one table.");
        assertTrue(tableNames.get(0).getObjectName().equals("nation"), "Expected 'nation' table.");

        assertThatQueryReturnsValue("INSERT INTO nation SELECT * FROM tpch.tiny.nation", 25L);

        assertThatQueryReturnsValue("INSERT INTO nation SELECT * FROM tpch.tiny.nation", 25L);

        assertThatQueryReturnsValue("SELECT count(*) FROM nation", 0L);

        assertThatQueryReturnsValue("DROP TABLE nation", true);
    }

    @Test
    public void notAllPropertiesSetForDataGeneration()
    {
        Session session = testSessionBuilder()
                .setCatalog("blackhole")
                .setSchema("default")
                .build();

        try {
            assertThatQueryReturnsValue(
                    format("CREATE TABLE nation WITH ( %s = 3, %s = 1 ) as SELECT * FROM tpch.tiny.nation",
                            ROWS_PER_PAGE_PROPERTY,
                            SPLIT_COUNT_PROPERTY),
                    25L,
                    session);
            fail("Expected exception to be thrown here!");
        }
        catch (RuntimeException ex) {
            // expected exception
        }
    }

    @Test
    public void createTableWithDistribution()
    {
        assertThatQueryReturnsValue(
                "CREATE TABLE distributed_test WITH ( distributed_on = array['orderkey'] ) AS SELECT * FROM tpch.tiny.orders",
                15000L);
        assertThatQueryReturnsValue("DROP TABLE distributed_test", true);
    }

    @Test
    public void dataGenerationUsage()
    {
        Session session = testSessionBuilder()
                .setCatalog("blackhole")
                .setSchema("default")
                .build();

        assertThatQueryReturnsValue(
                format("CREATE TABLE nation WITH ( %s = 3, %s = 2, %s = 1 ) as SELECT * FROM tpch.tiny.nation",
                        ROWS_PER_PAGE_PROPERTY,
                        PAGES_PER_SPLIT_PROPERTY,
                        SPLIT_COUNT_PROPERTY),
                25L,
                session);
        assertThatQueryReturnsValue("SELECT count(*) FROM nation", 6L, session);
        assertThatQueryReturnsValue("INSERT INTO nation SELECT * FROM tpch.tiny.nation", 25L, session);
        assertThatQueryReturnsValue("SELECT count(*) FROM nation", 6L, session);

        MaterializedResult rows = queryRunner.execute(session, "SELECT * FROM nation LIMIT 1");
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow row = Iterables.getOnlyElement(rows);
        assertEquals(row.getFieldCount(), 4);
        assertEquals(row.getField(0), 0L);
        assertEquals(row.getField(1), "****************");
        assertEquals(row.getField(2), 0L);
        assertEquals(row.getField(3), "****************");

        assertThatQueryReturnsValue("DROP TABLE nation", true);
    }

    @Test
    public void fieldLength()
    {
        Session session = testSessionBuilder()
                .setCatalog("blackhole")
                .setSchema("default")
                .build();

        assertThatQueryReturnsValue(
                format("CREATE TABLE nation WITH ( %s = 8, %s = 1, %s = 1, %s = 1 ) AS " +
                        "SELECT nationkey, name, regionkey, comment, 'abc' short_varchar FROM tpch.tiny.nation",
                        FIELD_LENGTH_PROPERTY,
                        ROWS_PER_PAGE_PROPERTY,
                        PAGES_PER_SPLIT_PROPERTY,
                        SPLIT_COUNT_PROPERTY),
                25L,
                session);

        MaterializedResult rows = queryRunner.execute(session, "SELECT * FROM nation");
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow row = Iterables.getOnlyElement(rows);
        assertEquals(row.getFieldCount(), 5);
        assertEquals(row.getField(0), 0L);
        assertEquals(row.getField(1), "********");
        assertEquals(row.getField(2), 0L);
        assertEquals(row.getField(3), "********");
        assertEquals(row.getField(4), "***"); // this one is shorter due to column type being VARCHAR(3)

        assertThatQueryReturnsValue("DROP TABLE nation", true);
    }

    private void assertThatNoBlackHoleTableIsCreated()
    {
        assertTrue(listBlackHoleTables().size() == 0, "No blackhole tables expected");
    }

    private List<QualifiedObjectName> listBlackHoleTables()
    {
        return queryRunner.listTables(queryRunner.getDefaultSession(), "blackhole", "default");
    }

    private void assertThatQueryReturnsValue(String sql, Object expected)
    {
        assertThatQueryReturnsValue(sql, expected, null);
    }

    private void assertThatQueryReturnsValue(String sql, Object expected, Session session)
    {
        MaterializedResult rows = session == null ? queryRunner.execute(sql) : queryRunner.execute(session, sql);
        MaterializedRow materializedRow = Iterables.getOnlyElement(rows);
        int fieldCount = materializedRow.getFieldCount();
        assertTrue(fieldCount == 1, format("Expected only one column, but got '%d'", fieldCount));
        Object value = materializedRow.getField(0);
        assertEquals(value, expected);
        assertTrue(Iterables.getOnlyElement(rows).getFieldCount() == 1);
    }
}
