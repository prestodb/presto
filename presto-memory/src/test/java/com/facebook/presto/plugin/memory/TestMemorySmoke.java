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

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.plugin.memory.MemoryQueryRunner.createQueryRunner;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
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
        queryRunner.execute("CREATE TABLE test AS SELECT * FROM tpch.tiny.nation");
        assertEquals(listMemoryTables().size(), tablesBeforeCreate + 1);

        queryRunner.execute(format("DROP TABLE test"));
        assertEquals(listMemoryTables().size(), tablesBeforeCreate);
    }

    @Test
    public void createTableWhenTableIsAlreadyCreated()
            throws SQLException
    {
        String createTableSql = "CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation";
        try {
            queryRunner.execute(createTableSql);
            fail("Expected exception to be thrown here!");
        }
        catch (RuntimeException ex) { // it has to be RuntimeException as FailureInfo$FailureException is private
            assertTrue(ex.getMessage().equals("line 1:1: Destination table 'memory.default.nation' already exists"));
        }
    }

    @Test
    public void select()
            throws SQLException
    {
        queryRunner.execute("CREATE TABLE test_select AS SELECT * FROM tpch.tiny.nation");

        assertQuery("SELECT * FROM test_select ORDER BY nationkey", "SELECT * FROM tpch.tiny.nation ORDER BY nationkey");

        assertQueryResult("INSERT INTO test_select SELECT * FROM tpch.tiny.nation", 25L);

        assertQueryResult("INSERT INTO test_select SELECT * FROM tpch.tiny.nation", 25L);

        assertQueryResult("SELECT count(*) FROM test_select", 75L);
    }

    @Test
    public void createTableWithNoData()
            throws SQLException
    {
        queryRunner.execute("CREATE TABLE test_empty (a BIGINT)");
        assertQueryResult("SELECT count(*) FROM test_empty", 0L);
        assertQueryResult("INSERT INTO test_empty SELECT nationkey FROM tpch.tiny.nation", 25L);
        assertQueryResult("SELECT count(*) FROM test_empty", 25L);
    }

    @Test
    public void createFilteredOutTable()
            throws SQLException
    {
        queryRunner.execute("CREATE TABLE filtered_out AS SELECT nationkey FROM tpch.tiny.nation WHERE nationkey < 0");
        assertQueryResult("SELECT count(*) FROM filtered_out", 0L);
        assertQueryResult("INSERT INTO filtered_out SELECT nationkey FROM tpch.tiny.nation", 25L);
        assertQueryResult("SELECT count(*) FROM filtered_out", 25L);
    }

    @Test
    public void selectFromEmptyTable()
            throws SQLException
    {
        queryRunner.execute("CREATE TABLE test_select_empty AS SELECT * FROM tpch.tiny.nation WHERE nationkey > 1000");

        assertQueryResult("SELECT count(*) FROM test_select_empty", 0L);
    }

    @Test
    public void selectSingleRow()
    {
        assertQuery("SELECT * FROM nation WHERE nationkey = 1", "SELECT * FROM tpch.tiny.nation WHERE nationkey = 1");
    }

    @Test
    public void selectColumnsSubset()
            throws SQLException
    {
        assertQuery("SELECT nationkey, regionkey FROM nation ORDER BY nationkey", "SELECT nationkey, regionkey FROM tpch.tiny.nation ORDER BY nationkey");
    }

    private List<QualifiedObjectName> listMemoryTables()
    {
        return queryRunner.listTables(queryRunner.getDefaultSession(), "memory", "default");
    }

    private void assertQueryResult(String sql, Object... expected)
    {
        MaterializedResult rows = queryRunner.execute(sql);
        assertEquals(rows.getRowCount(), expected.length);

        for (int i = 0; i < expected.length; i++) {
            MaterializedRow materializedRow = rows.getMaterializedRows().get(i);
            int fieldCount = materializedRow.getFieldCount();
            assertTrue(fieldCount == 1, format("Expected only one column, but got '%d'", fieldCount));
            Object value = materializedRow.getField(0);
            assertEquals(value, expected[i]);
            assertTrue(materializedRow.getFieldCount() == 1);
        }
    }

    private void assertQuery(String sql, String expected)
    {
        MaterializedResult rows = queryRunner.execute(sql);
        MaterializedResult expectedRows = queryRunner.execute(expected);

        assertEquals(rows, expectedRows);
    }
}
