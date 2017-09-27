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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.hive.HiveSessionProperties.RCFILE_OPTIMIZED_WRITER_ENABLED;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.tpch.TpchTable.getTables;

public class TestHiveDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestHiveDistributedQueries()
            throws Exception
    {
        super(() -> createQueryRunner(getTables()));
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Test
    public void testOrderByChar()
            throws Exception
    {
        assertUpdate("CREATE TABLE char_order_by (c_char char(2))");
        assertUpdate("INSERT INTO char_order_by (c_char) VALUES" +
                "(CAST('a' as CHAR(2)))," +
                "(CAST('a\0' as CHAR(2)))," +
                "(CAST('a  ' as CHAR(2)))", 3);

        MaterializedResult actual = computeActual(getSession(),
                "SELECT * FROM char_order_by ORDER BY c_char ASC");

        assertUpdate("DROP TABLE char_order_by");

        MaterializedResult expected = resultBuilder(getSession(), createCharType(2))
                .row("a\0")
                .row("a ")
                .row("a ")
                .build();

        assertEquals(actual, expected);
    }

    /**
     * Tests correctness of comparison of char(x) and varchar pushed down to a table scan as a TupleDomain
     */
    @Test
    public void testPredicatePushDownToTableScan()
            throws Exception
    {
        // Test not specific to Hive, but needs a connector supporting table creation

        assertUpdate("CREATE TABLE test_table_with_char (a char(20))");
        try {
            assertUpdate("INSERT INTO test_table_with_char (a) VALUES" +
                    "(cast('aaa' as char(20)))," +
                    "(cast('bbb' as char(20)))," +
                    "(cast('bbc' as char(20)))," +
                    "(cast('bbd' as char(20)))", 4);

            assertQuery(
                    "SELECT a, a <= 'bbc' FROM test_table_with_char",
                    "VALUES (cast('aaa' as char(20)), true), " +
                            "(cast('bbb' as char(20)), true), " +
                            "(cast('bbc' as char(20)), false), " +
                            "(cast('bbd' as char(20)), false)");

            assertQuery(
                    "SELECT a FROM test_table_with_char WHERE a <= 'bbc'",
                    "VALUES cast('aaa' as char(20)), " +
                            "cast('bbb' as char(20))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char");
        }
    }

    @Test
    public void testRcTextCharDecoding()
            throws Exception
    {
        testRcTextCharDecoding(false);
        testRcTextCharDecoding(true);
    }

    @Test
    public void testInvalidPartitionValue()
            throws Exception
    {
        assertUpdate("CREATE TABLE invalid_partition_value (a int, b varchar) WITH (partitioned_by = ARRAY['b'])");
        assertQueryFails(
                "INSERT INTO invalid_partition_value VALUES (4, '\n')",
                "Cannot use \n as value of a partition column in Hive. Partition keys in Hive can only contain characters in the range of 0x20 - 0x7E.");
        assertUpdate("DROP TABLE invalid_partition_value");

        assertQueryFails(
                "CREATE TABLE invalid_partition_value (a, b) WITH (partitioned_by = ARRAY['b']) AS SELECT 4, '\n'",
                "Cannot use \n as value of a partition column in Hive. Partition keys in Hive can only contain characters in the range of 0x20 - 0x7E.");
    }

    private void testRcTextCharDecoding(boolean rcFileOptimizedWriterEnabled)
            throws Exception
    {
        String catalog = getSession().getCatalog().get();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, RCFILE_OPTIMIZED_WRITER_ENABLED, Boolean.toString(rcFileOptimizedWriterEnabled))
                .build();

        assertUpdate(session, "CREATE TABLE test_table_with_char_rc WITH (format = 'RCTEXT') AS SELECT CAST('khaki' AS CHAR(7)) char_column", 1);
        try {
            assertQuery(session,
                    "SELECT * FROM test_table_with_char_rc WHERE char_column = 'khaki  '",
                    "VALUES (CAST('khaki' AS CHAR(7)))");
        }
        finally {
            assertUpdate(session, "DROP TABLE test_table_with_char_rc");
        }
    }
}
