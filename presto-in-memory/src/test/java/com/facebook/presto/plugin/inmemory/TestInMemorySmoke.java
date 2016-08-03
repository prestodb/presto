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

package com.facebook.presto.plugin.inmemory;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.Iterables;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.plugin.inmemory.InMemoryQueryRunner.createQueryRunner;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestInMemorySmoke
{
    private QueryRunner queryRunner;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @AfterMethod
    public void tearDown()
    {
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
            assertTrue(ex.getMessage().equals("line 1:1: Destination table 'inmemory.default.nation' already exists"));
        }
    }

    @Test
    public void inMemoryConnectorUsage()
            throws SQLException
    {
        assertThatQueryReturnsValue("CREATE TABLE nation as SELECT * FROM tpch.tiny.nation", 25L);

        List<QualifiedObjectName> tableNames = listInMemoryTables();
        assertTrue(tableNames.size() == 1, "Expected only one table.");
        assertTrue(tableNames.get(0).getObjectName().equals("nation"), "Expected 'nation' table.");

        assertThatQueryReturnsSameValueAs("SELECT * FROM nation ORDER BY nationkey", "SELECT * FROM tpch.tiny.nation ORDER BY nationkey");

        assertThatQueryReturnsValue("INSERT INTO nation SELECT * FROM tpch.tiny.nation", 25L);

        assertThatQueryReturnsValue("INSERT INTO nation SELECT * FROM tpch.tiny.nation", 25L);

        assertThatQueryReturnsValue("SELECT count(*) FROM nation", 75L);
    }

    @Test
    public void selectSingleRow()
            throws SQLException
    {
        assertThatQueryReturnsValue("CREATE TABLE nation as SELECT * FROM tpch.tiny.nation", 25L);

        assertThatQueryReturnsSameValueAs("SELECT * FROM nation WHERE nationkey = 1", "SELECT * FROM tpch.tiny.nation WHERE nationkey = 1");
    }

    private List<QualifiedObjectName> listInMemoryTables()
    {
        return queryRunner.listTables(queryRunner.getDefaultSession(), "inmemory", "default");
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

    private void assertThatQueryReturnsSameValueAs(String sql, String compareSql)
    {
        assertThatQueryReturnsSameValueAs(sql, compareSql, null);
    }

    private void assertThatQueryReturnsSameValueAs(String sql, String compareSql, Session session)
    {
        MaterializedResult rows = session == null ? queryRunner.execute(sql) : queryRunner.execute(session, sql);
        MaterializedResult expectedRows = session == null ? queryRunner.execute(compareSql) : queryRunner.execute(session, compareSql);

        assertEquals(rows, expectedRows);
    }
}
