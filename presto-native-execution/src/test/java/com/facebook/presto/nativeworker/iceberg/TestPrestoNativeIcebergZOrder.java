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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.facebook.presto.iceberg.util.ZOrderByteUtils.intToOrderedBytes;
import static com.facebook.presto.iceberg.util.ZOrderByteUtils.interleaveBits;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/// Verifies the native zorder function and z-order rewrites against Presto Java.
public class TestPrestoNativeIcebergZOrder
        extends AbstractTestQueryFramework
{
    // One row per null or edge value, covering every type zorder supports.
    private static final String ALL_TYPES_VALUES = "VALUES " +
            "(TINYINT '-3', SMALLINT '300', INTEGER '-70000', BIGINT '9000000000', REAL '1.5', DOUBLE '-2.25', " +
            "true, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00.123', 'abc', X'0102'), " +
            "(TINYINT '127', SMALLINT '-32768', INTEGER '0', BIGINT '-1', REAL '-0.0', nan(), " +
            "false, DATE '1960-03-01', TIMESTAMP '1960-03-01 00:00:00.001', 'a string that is longer than thirty-two bytes', X''), " +
            "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)";
    private static final String ALL_TYPES_COLUMNS = "t(c_tinyint, c_smallint, c_integer, c_bigint, c_real, c_double, " +
            "c_boolean, c_date, c_timestamp, c_varchar, c_varbinary)";
    private static final String ZORDER_ALL_TYPES = "SELECT to_hex(zorder(ROW(c_tinyint, c_smallint, c_integer, c_bigint, " +
            "c_real, c_double, c_boolean, c_date, c_timestamp, c_varchar, c_varbinary)))";
    // Generates rows whose a and b are scrambled, so a z-order rewrite has to reorder them.
    private static final String SOURCE_QUERY = "SELECT CAST(x * 37 %% 100 AS INTEGER) AS a, CAST(x * 53 %% 100 AS INTEGER) AS b, " +
            "CAST(x AS VARCHAR) AS payload FROM UNNEST(sequence(%s, %s)) t(x)";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testZOrderFunctionMatchesJava()
    {
        // Reading the values from a table makes the native workers compute zorder, rather than the
        // coordinator folding it over constants.
        assertUpdate("DROP TABLE IF EXISTS test_zorder_all_types");
        assertUpdate("CREATE TABLE test_zorder_all_types AS SELECT * FROM (" + ALL_TYPES_VALUES + ") " + ALL_TYPES_COLUMNS, 3);
        assertQuery(
                ZORDER_ALL_TYPES + " FROM test_zorder_all_types",
                ZORDER_ALL_TYPES + " FROM (" + ALL_TYPES_VALUES + ") " + ALL_TYPES_COLUMNS);
    }

    @Test
    public void testZOrderFunctionRejectsUnsupportedType()
    {
        assertUpdate("DROP TABLE IF EXISTS test_zorder_decimal");
        assertUpdate("CREATE TABLE test_zorder_decimal AS SELECT DECIMAL '1.50' AS d", 1);
        assertQueryFails("SELECT zorder(ROW(d)) FROM test_zorder_decimal", ".*Cannot use column of type .* in ZOrdering.*");
    }

    @Test
    public void testRewriteDataFilesWithZOrder()
    {
        String schema = getSession().getSchema().get();
        assertUpdate("DROP TABLE IF EXISTS test_zorder_rewrite");
        assertUpdate("CREATE TABLE test_zorder_rewrite (a INTEGER, b INTEGER, payload VARCHAR)");
        // Three inserts write three files.
        for (int insert = 0; insert < 3; insert++) {
            assertUpdate(format("INSERT INTO test_zorder_rewrite " + SOURCE_QUERY, insert * 1000, insert * 1000 + 999), 1000);
        }

        // rewrite-all includes groups below the default minimum of five input files.
        assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => 'test_zorder_rewrite', " +
                "strategy => 'sort', sorted_by => ARRAY['zorder(a, b)'], " +
                "options => map(ARRAY['rewrite-all'], ARRAY['true']))", schema), 3000);

        assertQuery("SELECT a, b, payload FROM test_zorder_rewrite", format(SOURCE_QUERY, 0, 2999));
        long rowCount = 0;
        for (String file : getDataFiles("test_zorder_rewrite")) {
            List<Group> rows = readRows(file);
            Slice previous = null;
            for (Group row : rows) {
                // The z-order value is a sort key only, so the file holds just the table columns.
                assertEquals(row.getType().getFieldCount(), 3, format("Unexpected columns in %s: %s", file, row.getType()));
                Slice zOrder = interleaveBits(new Slice[] {intToOrderedBytes(row.getInteger("a", 0)), intToOrderedBytes(row.getInteger("b", 0))}, 16);
                assertTrue(previous == null || previous.compareTo(zOrder) <= 0, format("Rows of %s are not in z-order: %s", file, row));
                previous = zOrder;
            }
            rowCount += rows.size();
        }
        assertEquals(rowCount, 3000);
    }

    private List<String> getDataFiles(String table)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\"", table)).getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
    }

    /// Reads the rows of a Parquet file in the order they are stored.
    private static List<Group> readRows(String file)
    {
        ImmutableList.Builder<Group> rows = ImmutableList.builder();
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                .withConf(new Configuration())
                .build()) {
            for (Group row = reader.read(); row != null; row = reader.read()) {
                rows.add(row);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return rows.build();
    }
}
