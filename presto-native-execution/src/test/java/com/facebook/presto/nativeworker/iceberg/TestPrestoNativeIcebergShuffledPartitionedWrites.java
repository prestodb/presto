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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/// Verifies that shuffle_partitioned_columns_for_table_write sends each partition to a single writer on native workers.
public class TestPrestoNativeIcebergShuffledPartitionedWrites
        extends AbstractTestQueryFramework
{
    private static final long PARTITION_COUNT = 10;
    private static final long ROW_COUNT = 8_000;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .build();
    }

    @BeforeClass
    public void createSourceTable()
    {
        // Four inserts write four files, each holding every partition value, so the source is read by several tasks.
        assertUpdate("DROP TABLE IF EXISTS shuffle_source");
        assertUpdate("CREATE TABLE shuffle_source (k INTEGER, d DECIMAL(10, 2), payload VARCHAR)");
        for (int insert = 0; insert < 4; insert++) {
            assertUpdate(format("INSERT INTO shuffle_source SELECT CAST(x %% %s AS INTEGER), CAST(x %% %s AS DECIMAL(10, 2)), CAST(x AS VARCHAR) " +
                    "FROM UNNEST(sequence(%s, %s)) t(x)", PARTITION_COUNT, PARTITION_COUNT, insert * 2000, insert * 2000 + 1999), ROW_COUNT / 4);
        }
    }

    @Test
    public void testCreateTableAsSelect()
    {
        // Without the shuffle, every writer that reads rows of a partition writes its own file for it.
        writeCopy("shuffle_ctas_default", "partitioning = ARRAY['k']", writerSession(false));
        long defaultFiles = countDataFiles("shuffle_ctas_default");
        assertTrue(defaultFiles > PARTITION_COUNT, format("Expected several files per partition without the shuffle, got %s files", defaultFiles));

        writeCopy("shuffle_ctas", "partitioning = ARRAY['k']", writerSession(true));
        assertEquals(countDataFiles("shuffle_ctas"), PARTITION_COUNT);
        assertSameRows("shuffle_ctas");
    }

    @Test
    public void testInsert()
    {
        assertUpdate("DROP TABLE IF EXISTS shuffle_insert");
        assertUpdate("CREATE TABLE shuffle_insert (k INTEGER, d DECIMAL(10, 2), payload VARCHAR) WITH (partitioning = ARRAY['k'])");
        assertUpdate(writerSession(true), "INSERT INTO shuffle_insert SELECT * FROM shuffle_source", ROW_COUNT);

        assertEquals(countDataFiles("shuffle_insert"), PARTITION_COUNT);
        assertSameRows("shuffle_insert");
    }

    @Test
    public void testNonIdentityPartitioningIsWrittenAsBefore()
    {
        // Hashing the source values of a transform would spread a partition over writers, so the shuffle is skipped.
        writeCopy("shuffle_bucket_transform", "partitioning = ARRAY['bucket(k, 4)']", writerSession(true));
        assertSameRows("shuffle_bucket_transform");
    }

    @Test
    public void testUnsupportedPartitionTypeIsWrittenAsBefore()
    {
        // The Hive-compatible bucket function does not hash DECIMAL, so the shuffle is skipped.
        writeCopy("shuffle_decimal_partition", "partitioning = ARRAY['d']", writerSession(true));
        assertSameRows("shuffle_decimal_partition");
    }

    private Session writerSession(boolean shufflePartitionedColumns)
    {
        // Several writer drivers per task make the unshuffled case write several files per partition reliably.
        return Session.builder(getSession())
                .setSystemProperty(TASK_WRITER_COUNT, "4")
                .setCatalogSessionProperty(ICEBERG_CATALOG, SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE, Boolean.toString(shufflePartitionedColumns))
                .build();
    }

    private void writeCopy(String table, String tableProperties, Session session)
    {
        assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        assertUpdate(session, format("CREATE TABLE %s WITH (%s) AS SELECT * FROM shuffle_source", table, tableProperties), ROW_COUNT);
    }

    private long countDataFiles(String table)
    {
        return (long) computeActual(format("SELECT count(*) FROM \"%s$files\"", table)).getOnlyValue();
    }

    private void assertSameRows(String table)
    {
        assertEqualsIgnoreOrder(
                computeActual(format("SELECT * FROM %s", table)).getMaterializedRows(),
                computeActual("SELECT * FROM shuffle_source").getMaterializedRows());
    }
}
