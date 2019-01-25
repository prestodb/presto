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
package io.prestosql.plugin.phoenix;

import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.tests.AbstractTestDistributedQueries;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

@Test
public class TestPhoenixDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestPhoenixDistributedQueries()
            throws Exception
    {
        super(() -> PhoenixQueryRunner.createPhoenixQueryRunner(ImmutableMap.of(), TpchTable.getTables()));
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    public void testRenameTable()
    {
        // Phoenix does not support renaming tables
    }

    @Override
    public void testRenameColumn()
    {
        // Phoenix does not support renaming columns
    }

    @Test
    public void testDeleteCompoundPk()
    {
        assertUpdate("CREATE TABLE test_delete_compound_pk  (pk1 bigint, pk2 varchar, val1 varchar) WITH (ROWKEYS='pk1,pk2')");

        assertUpdate("INSERT INTO test_delete_compound_pk VALUES (1, 'pkVal1', 'val1')", 1);
        assertUpdate("INSERT INTO test_delete_compound_pk VALUES (2, 'pkVal2', 'val2')", 1);
        assertUpdate("INSERT INTO test_delete_compound_pk VALUES (3, 'pkVal3', 'val3')", 1);

        assertUpdate("DELETE FROM test_delete_compound_pk where val1 = 'val2'", 1);
        assertQuery("SELECT count(*) FROM test_delete_compound_pk where val1 = 'val2'", "SELECT 0");

        assertUpdate("DELETE FROM test_delete_compound_pk where pk1 = 3 AND pk2 = 'pkVal3'", 1);
        assertQuery("SELECT pk1 from test_delete_compound_pk", "SELECT 1");
    }

    @Test
    public void testInsert()
    {
        @Language("SQL")
        String query = "SELECT orderdate, orderkey, totalprice FROM orders";

        assertUpdate("CREATE TABLE test_insert WITH (ROWKEYS='orderkey')AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM test_insert", query);

        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-2)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, orderdate) VALUES (-3, DATE '2001-01-01')", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, orderdate) VALUES (-4, DATE '2001-01-02')", 1);
        assertUpdate("INSERT INTO test_insert (orderdate, orderkey) VALUES (DATE '2001-01-03', -5)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, totalprice) VALUES (-6, 1234)", 1);

        assertQuery("SELECT * FROM test_insert", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT null, -2, null"
                + " UNION ALL SELECT DATE '2001-01-01', -3, null"
                + " UNION ALL SELECT DATE '2001-01-02', -4, null"
                + " UNION ALL SELECT DATE '2001-01-03', -5, null"
                + " UNION ALL SELECT null, -6, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO test_insert (orderkey, orderdate, totalprice) " +
                        "SELECT orderkey, orderdate, totalprice FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT 2 * count(*) FROM orders");

        assertUpdate("DROP TABLE test_insert");

        assertUpdate("CREATE TABLE test_insert (pk BIGINT WITH (primary_key=true), a ARRAY<DOUBLE>,  b ARRAY<BIGINT>)");

        assertUpdate("INSERT INTO test_insert (pk, a) VALUES (1, ARRAY[null])", 1);
        assertUpdate("INSERT INTO test_insert (pk, a) VALUES (2, ARRAY[1234])", 1);
        // An array of numeric primitive types returns 0 when the value is null.
        assertQuery("SELECT a[1] FROM test_insert", "VALUES (0), (1234)");

        assertQueryFails("INSERT INTO test_insert (pk, b) VALUES (3, ARRAY[1.23E1])", "Insert query has mismatched column types: .*");

        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testInsertDuplicateRows()
    {
        // Insert a row without specifying the comment column. That column will be null.
        // https://prestodb.io/docs/current/sql/insert.html
        try {
            assertUpdate("CREATE TABLE test_insert_duplicate WITH (ROWKEYS = 'a') AS SELECT 1 a, 2 b, '3' c", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, 2, '3'");
            assertUpdate("INSERT INTO test_insert_duplicate (a, c) VALUES (1, '4')", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, null, '4'");
            assertUpdate("INSERT INTO test_insert_duplicate (a, b) VALUES (1, 3)", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, 3, null");
        }
        finally {
            assertUpdate("DROP TABLE test_insert_duplicate");
        }
    }
}
