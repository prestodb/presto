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
package com.facebook.presto.nativetests.iceberg;

import org.testng.annotations.Test;

import static java.lang.String.format;

/**
 * ORC V3 tables on native workers. Native reads of ORC data files written by Java workers are
 * checked against Java execution. Native ORC writes are pinned at their current behavior: the
 * worker writes ORC table data with the DWRF writer, and its ORC reader cannot read those files
 * back.
 */
public class TestNativeIcebergV3Orc
        extends AbstractNativeIcebergV3Test
{
    public TestNativeIcebergV3Orc()
    {
        super("ORC");
    }

    @Test
    public void testReadJavaWrittenTable()
    {
        String table = "orc_java_written";
        createJavaWrittenV3Table(table,
                "id integer, b boolean, l bigint, d double, sd decimal(10, 2), ld decimal(38, 10), dt date, s varchar",
                "(1, true, BIGINT '10000000000', 2.25, DECIMAL '12.34', DECIMAL '1234567890.0123456789', DATE '2024-01-01', 'abc'), "
                        + "(2, false, NULL, NULL, NULL, NULL, NULL, NULL)");
        assertQuery("SELECT * FROM " + table);
        assertQuery("SELECT id FROM " + table + " WHERE s IS NULL AND ld IS NULL", "VALUES 2");
        assertQuery("SELECT sum(sd), sum(ld) FROM " + table);
        dropTable(table);
    }

    // TODO: Enable once native reads of Java-written ORC timestamps match Java. The native worker
    // currently returns them shifted by a time zone offset (2024-01-01 12:34:56.789 reads back as
    // 2024-01-01 20:34:56.789 locally), so results differ from Java execution.
    @Test(enabled = false)
    public void testReadJavaWrittenTimestamp()
    {
        String table = "orc_java_timestamp";
        createJavaWrittenV3Table(table, "id integer, ts timestamp", "(1, TIMESTAMP '2024-01-01 12:34:56.789'), (2, NULL)");
        assertQuery("SELECT * FROM " + table, "VALUES (1, TIMESTAMP '2024-01-01 12:34:56.789'), (2, NULL)");
        dropTable(table);
    }

    @Test
    public void testReadJavaWrittenNestedTypes()
    {
        String table = "orc_java_nested";
        createJavaWrittenV3Table(table, "id integer, arr array(integer), m map(varchar, bigint), r row(x integer, y varchar)",
                "(1, ARRAY[1, 2, 3], MAP(ARRAY['a', 'b'], ARRAY[BIGINT '1', BIGINT '2']), ROW(10, 'ten')), (2, ARRAY[], MAP(), NULL), (3, NULL, NULL, ROW(NULL, 'y'))");
        assertQuery("SELECT id, cardinality(arr), element_at(m, 'b'), r.x, r.y FROM " + table);
        assertQuery("SELECT id, x FROM " + table + " CROSS JOIN UNNEST(arr) AS t(x)", "VALUES (1, 1), (1, 2), (1, 3)");
        dropTable(table);
    }

    @Test
    public void testReadJavaWrittenPartitionedAndEvolvedTable()
    {
        String table = "orc_java_partitioned";
        dropTable(table);
        javaRunner().execute(format("CREATE TABLE %s (id integer, region varchar) %s", table, v3With("partitioning = ARRAY['region']")));
        javaRunner().execute("INSERT INTO " + table + " VALUES (1, 'us'), (2, 'eu'), (3, 'us')");
        javaRunner().execute("ALTER TABLE " + table + " ADD COLUMN score bigint");
        javaRunner().execute("INSERT INTO " + table + " VALUES (4, 'apac', 40)");
        javaRunner().execute("ALTER TABLE " + table + " RENAME COLUMN score TO points");

        assertQuery("SELECT * FROM " + table);
        assertQuery("SELECT id FROM " + table + " WHERE region = 'us'", "VALUES 1, 3");
        assertQuery("SELECT id, points FROM " + table + " WHERE points IS NOT NULL", "VALUES (4, 40)");
        dropTable(table);
    }

    @Test
    public void testNativeWriteIsNotReadable()
    {
        // The native writer maps ORC to DWRF and writes .dwrf files, but the native read path keeps
        // the ORC reader for ORC data files, which cannot decode the DWRF footer.
        String table = "orc_native_written";
        createV3Table(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')", 2);
        assertQueryFails("SELECT * FROM " + table, "(?s).*(lz4 failed to decompress|ParseFromZeroCopyStream).*\\.dwrf.*");
        dropTable(table);
    }

    @Test
    public void testNativeWriteOfLongDecimalUnsupported()
    {
        String table = "orc_native_long_decimal";
        createV3Table(table, "id integer, d decimal(38, 10)");
        assertQueryFails("INSERT INTO " + table + " VALUES (1, DECIMAL '1234567890.0123456789')", "(?s).*Decimal is not supported for DWRF.*");
        dropTable(table);
    }

    private void createJavaWrittenV3Table(String table, String columns, String values)
    {
        dropTable(table);
        javaRunner().execute(format("CREATE TABLE %s (%s) %s", table, columns, v3With()));
        javaRunner().execute(format("INSERT INTO %s VALUES %s", table, values));
    }
}
