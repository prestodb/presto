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

package com.facebook.presto.plugin.memory;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.Iterables;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.plugin.memory.MemoryQueryRunner.createQueryRunner;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestMemorySmoke
{
    private QueryRunner queryRunner;

    @BeforeTest
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @Test
    public void createAndDropTable()
            throws SQLException
    {
        int tablesBeforeCreate = listMemoryTables().size();
        queryRunner.execute("CREATE TABLE test as SELECT * FROM tpch.tiny.nation");
        assertEquals(listMemoryTables().size(), tablesBeforeCreate + 1);

        queryRunner.execute(format("DROP TABLE test"));
        assertEquals(listMemoryTables().size(), tablesBeforeCreate);
    }

    @Test
    public void createTableWhenTableIsAlreadyCreated()
            throws SQLException
    {
        String createTableSql = "CREATE TABLE nation as SELECT * FROM tpch.tiny.nation";
        try {
            queryRunner.execute(createTableSql);
            fail("Expected exception to be thrown here!");
        }
        catch (RuntimeException ex) { // it has to RuntimeException as FailureInfo$FailureException is private
            assertTrue(ex.getMessage().equals("line 1:1: Destination table 'memory.default.nation' already exists"));
        }
    }

    @Test
    public void select()
            throws SQLException
    {
        queryRunner.execute("CREATE TABLE test_select as SELECT * FROM tpch.tiny.nation");

        assertThatQueryReturnsSameValueAs("SELECT * FROM test_select ORDER BY nationkey", "SELECT * FROM tpch.tiny.nation ORDER BY nationkey");

        assertThatQueryReturnsValue("INSERT INTO test_select SELECT * FROM tpch.tiny.nation", 25L);

        assertThatQueryReturnsValue("INSERT INTO test_select SELECT * FROM tpch.tiny.nation", 25L);

        assertThatQueryReturnsValue("SELECT count(*) FROM test_select", 75L);
    }

    @Test
    public void selectFromEmptyTable()
            throws SQLException
    {
        queryRunner.execute("CREATE TABLE test_select_empty as SELECT * FROM tpch.tiny.nation WHERE nationkey > 1000");

        assertThatQueryReturnsValue("SELECT count(*) FROM test_select_empty", 0L);
    }

    @Test
    public void selectSingleRow()
    {
        assertThatQueryReturnsSameValueAs("SELECT * FROM nation WHERE nationkey = 1", "SELECT * FROM tpch.tiny.nation WHERE nationkey = 1");
    }

    @Test
    public void selectColumnsSubset()
            throws SQLException
    {
        assertThatQueryReturnsSameValueAs("SELECT nationkey, regionkey FROM nation ORDER BY nationkey", "SELECT nationkey, regionkey FROM tpch.tiny.nation ORDER BY nationkey");
    }

    private List<QualifiedObjectName> listMemoryTables()
    {
        return queryRunner.listTables(queryRunner.getDefaultSession(), "memory", "default");
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
