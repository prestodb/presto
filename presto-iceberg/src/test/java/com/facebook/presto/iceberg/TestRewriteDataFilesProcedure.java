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
package com.facebook.presto.iceberg;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterator;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DELETE_FILES_PROP;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.testng.Assert.assertEquals;

public class TestRewriteDataFilesProcedure
        extends AbstractTestQueryFramework
{
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    public void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testRewriteDataFilesInEmptyTable()
    {
        String tableName = "default_empty_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
            assertUpdate(format("CALL system.rewrite_data_files('%s', '%s')", TEST_SCHEMA, tableName), 0);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesOnPartitionTable()
    {
        String tableName = "example_partition_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) with (partitioning = ARRAY['c2'])");

            // create 5 files for each partition (c2 = 'foo' and c2 = 'bar')
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

            Table table = loadTable(tableName);
            assertHasSize(table.snapshots(), 5);
            //The number of data files is 10，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 10);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            CloseableIterator<FileScanTask> fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 10, 0);

            assertUpdate("DELETE from " + tableName + " WHERE c1 = 7", 1);
            assertUpdate("DELETE from " + tableName + " WHERE c1 in (8, 10)", 2);

            table.refresh();
            assertHasSize(table.snapshots(), 7);
            //The number of data files is 10，and the number of delete files is 3
            assertHasDataFiles(table.currentSnapshot(), 10);
            assertHasDeleteFiles(table.currentSnapshot(), 3);
            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(9, 'foo')");

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['1']))", tableName, TEST_SCHEMA), 7);

            table.refresh();
            assertHasSize(table.snapshots(), 8);
            //The number of data files is 2，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 2);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            fileScanTasks = table.newScan()
                    .filter(alwaysTrue())
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 2, 0);
            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(9, 'foo')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesOnNonPartitionTable()
    {
        String tableName = "example_non_partition_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");

            // create 5 files
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

            Table table = loadTable(tableName);
            assertHasSize(table.snapshots(), 5);
            //The number of data files is 5，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 5);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            CloseableIterator<FileScanTask> fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 5, 0);

            assertUpdate("DELETE from " + tableName + " WHERE c1 = 7", 1);
            assertUpdate("DELETE from " + tableName + " WHERE c1 in (9, 10)", 2);

            table.refresh();
            assertHasSize(table.snapshots(), 7);
            //The number of data files is 5，and the number of delete files is 2
            assertHasDataFiles(table.currentSnapshot(), 5);
            assertHasDeleteFiles(table.currentSnapshot(), 2);
            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(8, 'bar')");

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['1']))", tableName, TEST_SCHEMA), 7);

            table.refresh();
            assertHasSize(table.snapshots(), 8);
            //The number of data files is 1，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 1);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            fileScanTasks = table.newScan()
                    .filter(alwaysTrue())
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 1, 0);
            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(8, 'bar')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithFilter()
    {
        String tableName = "example_partition_filter_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) with (partitioning = ARRAY['c2'])");

            // create 5 files for each partition (c2 = 'foo' and c2 = 'bar')
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

            Table table = loadTable(tableName);
            assertHasSize(table.snapshots(), 5);
            //The number of data files is 10，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 10);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            CloseableIterator<FileScanTask> fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 10, 0);

            // do not support rewrite files filtered by non-identity columns
            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3')", tableName, TEST_SCHEMA), ".*");

            // select 5 files to rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 = ''bar''', options => map(array['min-input-files'], array['1']))", tableName, TEST_SCHEMA), 5);
            table.refresh();
            assertHasSize(table.snapshots(), 6);
            //The number of data files is 6，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 6);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 6, 0);

            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(7, 'foo'), (8, 'bar'), " +
                            "(9, 'foo'), (10, 'bar')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithDeterministicTrueFilter()
    {
        String tableName = "example_non_partition_true_filter_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");

            // create 5 files
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

            Table table = loadTable(tableName);
            assertHasSize(table.snapshots(), 5);
            //The number of data files is 5，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 5);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            CloseableIterator<FileScanTask> fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 5, 0);

            // do not support rewrite files filtered by non-identity columns
            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3')", tableName, TEST_SCHEMA), ".*");

            // the filter is `true` means select all files to rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 = 1', options => map(array['min-input-files'], array['1']))", tableName, TEST_SCHEMA), 10);

            table.refresh();
            assertHasSize(table.snapshots(), 6);
            //The number of data files is 1，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 1);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 1, 0);

            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(7, 'foo'), (8, 'bar'), " +
                            "(9, 'foo'), (10, 'bar')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithDeterministicFalseFilter()
    {
        String tableName = "example_non_partition_false_filter_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");

            // create 5 files
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

            Table table = loadTable(tableName);
            assertHasSize(table.snapshots(), 5);
            //The number of data files is 5，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 5);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            CloseableIterator<FileScanTask> fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 5, 0);

            // the filter is `false` means select no file to rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 = 0')", tableName, TEST_SCHEMA), 0);

            table.refresh();
            assertHasSize(table.snapshots(), 5);
            //The number of data files is still 5，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 5);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 5, 0);

            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(7, 'foo'), (8, 'bar'), " +
                            "(9, 'foo'), (10, 'bar')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithDeleteAndPartitionEvolution()
    {
        String tableName = "example_partition_evolution_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (a int, b varchar)");
            assertUpdate("INSERT INTO " + tableName + " values(1, '1001'), (2, '1002')", 2);
            assertUpdate("DELETE FROM " + tableName + " WHERE a = 1", 1);
            assertQuery("select * from " + tableName, "values(2, '1002')");

            Table table = loadTable(tableName);
            assertHasSize(table.snapshots(), 2);
            //The number of data files is 1，and the number of delete files is 1
            assertHasDataFiles(table.currentSnapshot(), 1);
            assertHasDeleteFiles(table.currentSnapshot(), 1);

            assertUpdate("alter table " + tableName + " add column c int with (partitioning = 'identity')");
            assertUpdate("INSERT INTO " + tableName + " values(5, '1005', 5), (6, '1006', 6), (7, '1007', 7)", 3);
            assertUpdate("DELETE FROM " + tableName + " WHERE b = '1006'", 1);
            assertQuery("select * from " + tableName, "values(2, '1002', NULL), (5, '1005', 5), (7, '1007', 7)");

            table.refresh();
            assertHasSize(table.snapshots(), 4);
            //The number of data files is 4，and the number of delete files is 2
            assertHasDataFiles(table.currentSnapshot(), 4);
            assertHasDeleteFiles(table.currentSnapshot(), 2);

            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'a > 3')", tableName, TEST_SCHEMA), ".*");
            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c > 3')", tableName, TEST_SCHEMA), ".*");

            // Set min-input-files=1 since we have 4 files across multiple partitions
            assertUpdate(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['1']))", tableName, TEST_SCHEMA), 3);
            table.refresh();
            assertHasSize(table.snapshots(), 5);
            //The number of data files is 3，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 3);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            CloseableIterator<FileScanTask> fileScanTasks = table.newScan()
                    .useSnapshot(table.currentSnapshot().snapshotId())
                    .planFiles().iterator();
            assertFilesPlan(fileScanTasks, 3, 0);
            assertQuery("select * from " + tableName, "values(2, '1002', NULL), (5, '1005', 5), (7, '1007', 7)");

            assertUpdate("delete from " + tableName + " where b = '1002'", 1);
            table.refresh();
            assertHasSize(table.snapshots(), 6);
            //The number of data files is 3，and the number of delete files is 1
            assertHasDataFiles(table.currentSnapshot(), 3);
            assertHasDeleteFiles(table.currentSnapshot(), 1);
            // Set min-input-files=1 to allow rewrite with filter
            assertUpdate(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c is null', options => map(array['min-input-files'], array['1']))", tableName, TEST_SCHEMA), 0);

            table.refresh();
            assertHasSize(table.snapshots(), 7);
            //The number of data files is 2，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 2);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            assertQuery("select * from " + tableName, "values(5, '1005', 5), (7, '1007', 7)");

            // This is a metadata delete
            assertUpdate("delete from " + tableName + " where c = 7", 1);
            table.refresh();
            assertHasSize(table.snapshots(), 8);
            //The number of data files is 1，and the number of delete files is 0
            assertHasDataFiles(table.currentSnapshot(), 1);
            assertHasDeleteFiles(table.currentSnapshot(), 0);
            assertQuery("select * from " + tableName, "values(5, '1005', 5)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidParameterCases()
    {
        String tableName = "invalid_parameter_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (a int, b varchar, c int)");
            assertQueryFails("CALL system.rewrite_data_files('n', table_name => 't')", ".*Named and positional arguments cannot be mixed");
            assertQueryFails("CALL custom.rewrite_data_files('n', 't')", "Procedure not registered: custom.rewrite_data_files");
            assertQueryFails("CALL system.rewrite_data_files()", ".*Required procedure argument 'schema' is missing");
            assertQueryFails("CALL system.rewrite_data_files('s', 'n')", "Schema s does not exist");
            assertQueryFails("CALL system.rewrite_data_files('', '')", "Table name is empty");
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '''hello''')", tableName, TEST_SCHEMA), ".*WHERE clause must evaluate to a boolean: actual type varchar\\(5\\)");
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1001')", tableName, TEST_SCHEMA), ".*WHERE clause must evaluate to a boolean: actual type integer");
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'a')", tableName, TEST_SCHEMA), ".*WHERE clause must evaluate to a boolean: actual type integer");
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'n')", tableName, TEST_SCHEMA), ".*Column 'n' cannot be resolved");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithMinInputFilesOption()
    {
        String tableName = "test_min_input_files_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar)");

            // Insert data to create 3 files
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 3);

            // Test with min-input-files = 5 (should skip rewrite since we only have 3 files)
            // When filtering prevents rewrite, no splits are generated and 0 rows are returned
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['5']))", tableName, TEST_SCHEMA), 0);
            table.refresh();
            // Should still have 3 files since rewrite was skipped due to min-input-files threshold
            assertHasDataFiles(table.currentSnapshot(), 3);

            // Test with min-input-files = 2 (should perform rewrite since we have 3 files >= 2)
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['2']))", tableName, TEST_SCHEMA), 3);
            table.refresh();
            // Should have 1 file after rewrite
            assertHasDataFiles(table.currentSnapshot(), 1);

            // Verify data integrity
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
        finally {
            dropTable(tableName);
        }
    }
    @Test
    public void testRewriteDataFilesWithInvalidMinInputFilesOption()
    {
        String tableName = "test_invalid_min_input_files";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            // Test with invalid min-input-files value (not a number)
            assertQueryFails(
                    format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['invalid']))", tableName, TEST_SCHEMA),
                    ".*min-input-files must be a valid integer.*");

            // Test with min-input-files less than 1
            assertQueryFails(
                    format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['0']))", tableName, TEST_SCHEMA),
                    ".*min-input-files must be at least 1.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMinInputFilesWithPartitioningCombineAll()
    {
        String tableName = "test_min_input_files_partitioned_combine";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar, value varchar) WITH (partitioning = ARRAY['category'])");

            // Create 3 files in partition 'A' and 3 files in partition 'B'
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 'val1')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'A', 'val2')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'A', 'val3')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'B', 'val4')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'B', 'val5')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, 'B', 'val6')", 1);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 6);

            // Rewrite all partitions with min-input-files = 2 (both partitions have 3 files >= 2)
            // This should combine all 6 files into 2 files (1 per partition)
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['2']))", tableName, TEST_SCHEMA), 6);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 2);

            // Verify data integrity
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 'val1'), (2, 'A', 'val2'), (3, 'A', 'val3'), (4, 'B', 'val4'), (5, 'B', 'val5'), (6, 'B', 'val6')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMinInputFilesWithPartitioningSelectiveRewrite()
    {
        String tableName = "test_min_input_files_partitioned_selective";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar, value varchar) WITH (partitioning = ARRAY['category'])");

            // Create 4 files in partition 'A', 2 files in partition 'B', and 3 files in partition 'C'
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 'val1')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'A', 'val2')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'A', 'val3')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'A', 'val4')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'B', 'val5')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, 'B', 'val6')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (7, 'C', 'val7')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (8, 'C', 'val8')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (9, 'C', 'val9')", 1);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 9);

            // Rewrite with min-input-files = 3
            // Partition A has 4 files (>= 3) - should be rewritten to 1 file
            // Partition B has 2 files (< 3) - should NOT be rewritten (stays as 2 files)
            // Partition C has 3 files (>= 3) - should be rewritten to 1 file
            // Total: 4 + 3 = 7 files rewritten, resulting in 1 + 2 + 1 = 4 files total
            // Row count now correctly returns 7 (rows from qualifying partitions) because
            // filtering happens at split generation time
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['3']))", tableName, TEST_SCHEMA), 7);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 4);

            // Verify data integrity
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 'val1'), (2, 'A', 'val2'), (3, 'A', 'val3'), (4, 'A', 'val4'), (5, 'B', 'val5'), (6, 'B', 'val6'), (7, 'C', 'val7'), (8, 'C', 'val8'), (9, 'C', 'val9')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMinInputFilesWithFilterAndPartitioningSelectiveRewrite()
    {
        String tableName = "test_min_input_files_filter_partitioned";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar, value varchar) WITH (partitioning = ARRAY['category'])");

            // Create multiple files across different partitions
            // Partition 'A': 4 files
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 'val1')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'A', 'val2')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'A', 'val3')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'A', 'val4')", 1);
            // Partition 'B': 2 files
            assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'B', 'val5')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (6, 'B', 'val6')", 1);
            // Partition 'C': 3 files
            assertUpdate("INSERT INTO " + tableName + " VALUES (7, 'C', 'val7')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (8, 'C', 'val8')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (9, 'C', 'val9')", 1);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 9);

            // Filter to only partitions 'A' and 'B', with min-input-files = 3
            // Partition A has 4 files (>= 3) - should be rewritten
            // Partition B has 2 files (< 3) - should NOT be rewritten
            // Partition C is filtered out - not considered
            // Result: Only partition A's 4 files are rewritten to 1 file
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'category IN (''A'', ''B'')', options => map(array['min-input-files'], array['3']))", tableName, TEST_SCHEMA), 4);
            table.refresh();
            // Total files: 1 (A rewritten) + 2 (B unchanged) + 3 (C unchanged) = 6
            assertHasDataFiles(table.currentSnapshot(), 6);

            // Verify data integrity
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 'val1'), (2, 'A', 'val2'), (3, 'A', 'val3'), (4, 'A', 'val4'), (5, 'B', 'val5'), (6, 'B', 'val6'), (7, 'C', 'val7'), (8, 'C', 'val8'), (9, 'C', 'val9')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMinInputFilesWithDeleteFiles()
    {
        String tableName = "test_min_input_files_with_deletes";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar)");

            // Insert multiple rows in single INSERT to create files that can have position deletes
            assertUpdate("INSERT INTO " + tableName + " values(1, 'a'), (2, 'b')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'c'), (4, 'd')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'e'), (6, 'f')", 2);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 3);
            assertHasDeleteFiles(table.currentSnapshot(), 0);

            // Delete one row to create a delete file
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 3);
            assertHasDeleteFiles(table.currentSnapshot(), 1);

            // Test with min-input-files = 5
            // We have 3 data files and 1 delete file
            // Only data files count toward threshold, so 3 < 5, should skip rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['5']))", tableName, TEST_SCHEMA), 0);
            table.refresh();
            // Should still have 3 data files and 1 delete file since rewrite was skipped
            assertHasDataFiles(table.currentSnapshot(), 3);
            assertHasDeleteFiles(table.currentSnapshot(), 1);

            // Test with min-input-files = 2
            // We have 3 data files (>= 2), should perform rewrite
            // Delete files should be applied during rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['2']))", tableName, TEST_SCHEMA), 5);
            table.refresh();
            // Should have 1 data file after rewrite, and 0 delete files (applied during rewrite)
            assertHasDataFiles(table.currentSnapshot(), 1);
            assertHasDeleteFiles(table.currentSnapshot(), 0);

            // Verify data integrity - row with id=1 should be deleted
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMinInputFilesWithDeleteFilesPartitioned()
    {
        String tableName = "test_min_input_files_deletes_partitioned";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar, value varchar) WITH (partitioning = ARRAY['category'])");

            // Create 4 data files in partition 'A' and 2 data files in partition 'B'
            // Use multi-row inserts to enable position deletes
            assertUpdate("INSERT INTO " + tableName + " values(1, 'A', 'val1'), (2, 'A', 'val2')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'A', 'val3'), (4, 'A', 'val4')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'A', 'val5'), (6, 'A', 'val6')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'A', 'val7'), (8, 'A', 'val8')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'B', 'val9'), (10, 'B', 'val10')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(11, 'B', 'val11'), (12, 'B', 'val12')", 2);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 6);
            assertHasDeleteFiles(table.currentSnapshot(), 0);

            // Delete rows to create delete files in both partitions
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 9", 1);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 6);
            assertHasDeleteFiles(table.currentSnapshot(), 2);

            // Rewrite with min-input-files = 3
            // Partition A: 4 data files + 1 delete file, but only 4 data files count (>= 3) - should rewrite
            // Partition B: 2 data files + 1 delete file, but only 2 data files count (< 3) - should NOT rewrite
            // Result: 7 rows from partition A (after applying delete)
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['3']))", tableName, TEST_SCHEMA), 7);
            table.refresh();
            // Partition A: 1 data file, 0 delete files (rewritten and delete applied)
            // Partition B: 2 data files, 1 delete file (unchanged)
            // Total: 3 data files, 1 delete file
            assertHasDataFiles(table.currentSnapshot(), 3);
            assertHasDeleteFiles(table.currentSnapshot(), 1);

            // Verify data integrity
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (2, 'A', 'val2'), (3, 'A', 'val3'), (4, 'A', 'val4'), (5, 'A', 'val5'), (6, 'A', 'val6'), (7, 'A', 'val7'), (8, 'A', 'val8'), (10, 'B', 'val10'), (11, 'B', 'val11'), (12, 'B', 'val12')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMinInputFilesWithMultipleDeleteFiles()
    {
        String tableName = "test_min_input_files_multiple_deletes";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar)");

            // Insert data to create 2 data files with multiple rows each
            assertUpdate("INSERT INTO " + tableName + " values(1, 'a'), (2, 'b')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'c'), (4, 'd')", 2);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 2);
            assertHasDeleteFiles(table.currentSnapshot(), 0);

            // Create multiple delete files
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'e'), (6, 'f')", 2);
            assertUpdate("DELETE FROM " + tableName + " WHERE value = 'b'", 1);

            table.refresh();
            // Should have 3 data files and 2 delete files
            assertHasDataFiles(table.currentSnapshot(), 3);
            assertHasDeleteFiles(table.currentSnapshot(), 2);

            // Test with min-input-files = 4
            // We have 3 data files and 2 delete files
            // Only 3 data files count (< 4), should skip rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['4']))", tableName, TEST_SCHEMA), 0);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 3);
            assertHasDeleteFiles(table.currentSnapshot(), 2);

            // Test with min-input-files = 3
            // We have 3 data files (>= 3), should perform rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['min-input-files'], array['3']))", tableName, TEST_SCHEMA), 4);
            table.refresh();
            // Should have 1 data file and 0 delete files after rewrite
            assertHasDataFiles(table.currentSnapshot(), 1);
            assertHasDeleteFiles(table.currentSnapshot(), 0);

            // Verify data integrity - rows with id=1 and id=2 should be deleted
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMinInputFilesWithDeletesPartitioningAndFilter()
    {
        String tableName = "test_min_input_files_comprehensive";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar, subcategory varchar, value varchar) WITH (partitioning = ARRAY['category'])");

            // Create multiple files across different partitions
            // Partition 'A': 5 data files
            assertUpdate("INSERT INTO " + tableName + " values(1, 'A', 'X', 'val1'), (2, 'A', 'X', 'val2')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'A', 'X', 'val3'), (4, 'A', 'X', 'val4')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'A', 'Y', 'val5'), (6, 'A', 'Y', 'val6')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'A', 'Y', 'val7'), (8, 'A', 'Y', 'val8')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'A', 'Z', 'val9'), (10, 'A', 'Z', 'val10')", 2);
            // Partition 'B': 2 data files
            assertUpdate("INSERT INTO " + tableName + " values(11, 'B', 'X', 'val11'), (12, 'B', 'X', 'val12')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(13, 'B', 'Y', 'val13'), (14, 'B', 'Y', 'val14')", 2);
            // Partition 'C': 4 data files
            assertUpdate("INSERT INTO " + tableName + " values(15, 'C', 'X', 'val15'), (16, 'C', 'X', 'val16')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(17, 'C', 'Y', 'val17'), (18, 'C', 'Y', 'val18')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(19, 'C', 'Z', 'val19'), (20, 'C', 'Z', 'val20')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(21, 'C', 'Z', 'val21'), (22, 'C', 'Z', 'val22')", 2);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 11);
            assertHasDeleteFiles(table.currentSnapshot(), 0);

            // Delete rows to create delete files in partitions A and C
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 15", 1);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 11);
            assertHasDeleteFiles(table.currentSnapshot(), 2);

            // Test comprehensive scenario:
            // - Filter: category IN ('A', 'B') - excludes partition C
            // - min-input-files: 3
            // - Partition A: 5 data files + 1 delete file, 5 >= 3 - should rewrite (9 rows after delete)
            // - Partition B: 2 data files + 0 delete files, 2 < 3 - should NOT rewrite
            // - Partition C: filtered out by WHERE clause - not considered
            // Result: Only partition A's 9 rows (after applying delete) should be rewritten
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'category IN (''A'', ''B'')', options => map(array['min-input-files'], array['3']))", tableName, TEST_SCHEMA), 9);
            table.refresh();
            // Partition A: 1 data file, 0 delete files (rewritten and delete applied)
            // Partition B: 2 data files, 0 delete files (unchanged)
            // Partition C: 4 data files, 1 delete file (unchanged, filtered out)
            // Total: 7 data files, 1 delete file
            assertHasDataFiles(table.currentSnapshot(), 7);
            assertHasDeleteFiles(table.currentSnapshot(), 1);

            // Verify data integrity - both deletes are visible in queries (Iceberg applies delete files during reads)
            // row with id=1 deleted (applied during rewrite of A), row with id=15 deleted (delete file still exists for C)
            assertQuery("SELECT COUNT(*) FROM " + tableName, "VALUES 20");
            assertQuery("SELECT COUNT(*) FROM " + tableName + " WHERE category = 'A'", "VALUES 9");
            assertQuery("SELECT COUNT(*) FROM " + tableName + " WHERE category = 'B'", "VALUES 4");
            assertQuery("SELECT COUNT(*) FROM " + tableName + " WHERE category = 'C'", "VALUES 7");

            // Now rewrite partition C with min-input-files = 4 (it has 4 files, exactly at threshold)
            // Returns 7 rows (the actual data after applying the delete file for id=15)
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'category = ''C''', options => map(array['min-input-files'], array['4']))", tableName, TEST_SCHEMA), 7);
            table.refresh();
            // Partition A: 1 data file, 0 delete files
            // Partition B: 2 data files, 0 delete files
            // Partition C: 1 data file, 0 delete files (rewritten and delete applied)
            // Total: 4 data files, 0 delete files
            assertHasDataFiles(table.currentSnapshot(), 4);
            assertHasDeleteFiles(table.currentSnapshot(), 0);

            // Verify final data integrity - both deletes fully applied (delete files removed)
            assertQuery("SELECT COUNT(*) FROM " + tableName, "VALUES 20");
            assertQuery("SELECT COUNT(*) FROM " + tableName + " WHERE category = 'A'", "VALUES 9");
            assertQuery("SELECT COUNT(*) FROM " + tableName + " WHERE category = 'B'", "VALUES 4");
            assertQuery("SELECT COUNT(*) FROM " + tableName + " WHERE category = 'C'", "VALUES 7");
        }
        finally {
            dropTable(tableName);
        }
    }
    @Test
    public void testRewriteDataFilesWithMinFileSizeBytes()
    {
        String tableName = "test_min_file_size_bytes";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, data varchar)");

            // Create two small files with 1 row each
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            // Create one large file with many rows
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(3, 'data'), (4, 'data'), (5, 'data'), (6, 'data'), (7, 'data'), " +
                    "(8, 'data'), (9, 'data'), (10, 'data'), (11, 'data'), (12, 'data')", 10);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 3);

            // Get actual file sizes to determine appropriate threshold
            List<Long> fileSizes = new ArrayList<>();
            for (FileScanTask task : table.newScan().planFiles()) {
                fileSizes.add(task.file().fileSizeInBytes());
            }
            Collections.sort(fileSizes);

            // Use a threshold between the small files and the large file
            // fileSizes should be: [small, small, large]
            long threshold = (fileSizes.get(1) + fileSizes.get(2)) / 2;

            // min-file-size-bytes filters files BELOW threshold (too small)
            // Should select the two small files and combine them
            // Set max-file-size-bytes to Long.MAX_VALUE to disable the default maximum filter
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['min-input-files', 'min-file-size-bytes', 'max-file-size-bytes'], array['1', '%d', '%d']))", TEST_SCHEMA, tableName, threshold, Long.MAX_VALUE), 2);

            table.refresh();
            // Two small files combined into one, large file unchanged = 2 total files
            assertHasDataFiles(table.currentSnapshot(), 2);

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'a'), (2, 'b'), (3, 'data'), (4, 'data'), (5, 'data'), " +
                    "(6, 'data'), (7, 'data'), (8, 'data'), (9, 'data'), (10, 'data'), (11, 'data'), (12, 'data')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithMaxFileSizeBytes()
    {
        String tableName = "test_max_file_size_bytes";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, data varchar)");

            // Create one small file and two large files
            // Small file
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            // Two large files with many rows each
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(2, 'data'), (3, 'data'), (4, 'data'), (5, 'data'), (6, 'data')", 5);
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(7, 'data'), (8, 'data'), (9, 'data'), (10, 'data'), (11, 'data')", 5);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 3);

            // Get actual file sizes to determine appropriate threshold
            List<Long> fileSizes = new ArrayList<>();
            for (FileScanTask task : table.newScan().planFiles()) {
                fileSizes.add(task.file().fileSizeInBytes());
            }
            Collections.sort(fileSizes);

            // Use a threshold between the small file and the large files
            // fileSizes should be: [small, large, large]
            long threshold = (fileSizes.get(0) + fileSizes.get(1)) / 2;

            // max-file-size-bytes filters files ABOVE threshold (too large)
            // Should select the two large files and combine them
            // Set min-file-size-bytes=0 to disable the default minimum filter
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['min-input-files', 'min-file-size-bytes', 'max-file-size-bytes'], array['1', '0', '%d']))", TEST_SCHEMA, tableName, threshold), 10);

            table.refresh();
            // Two large files combined into one, small file unchanged = 2 total files
            assertHasDataFiles(table.currentSnapshot(), 2);

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'a'), (2, 'data'), (3, 'data'), (4, 'data'), (5, 'data'), " +
                    "(6, 'data'), (7, 'data'), (8, 'data'), (9, 'data'), (10, 'data'), (11, 'data')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithMinAndMaxFileSizeBytes()
    {
        String tableName = "test_min_and_max_file_size_bytes";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, data varchar)");

            // Create files with three distinct size ranges: small, medium, large
            // Two small files (1 row each)
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            // Two medium files (5 rows each) - these should be skipped
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(3, 'data'), (4, 'data'), (5, 'data'), (6, 'data'), (7, 'data')", 5);
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(8, 'data'), (9, 'data'), (10, 'data'), (11, 'data'), (12, 'data')", 5);
            // One large file (15 rows)
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(13, 'data'), (14, 'data'), (15, 'data'), (16, 'data'), (17, 'data'), " +
                    "(18, 'data'), (19, 'data'), (20, 'data'), (21, 'data'), (22, 'data'), " +
                    "(23, 'data'), (24, 'data'), (25, 'data'), (26, 'data'), (27, 'data')", 15);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 5);

            // Get actual file sizes and categorize them
            List<Long> fileSizes = new ArrayList<>();
            for (FileScanTask task : table.newScan().planFiles()) {
                fileSizes.add(task.file().fileSizeInBytes());
            }
            Collections.sort(fileSizes);

            // Set thresholds to select small and large files, but skip medium files
            // fileSizes should be: [small, small, medium, medium, large]
            long minThreshold = (fileSizes.get(1) + fileSizes.get(2)) / 2; // Between small and medium
            long maxThreshold = (fileSizes.get(3) + fileSizes.get(4)) / 2; // Between medium and large

            // min-file-size-bytes selects files < minThreshold (the 2 small files)
            // max-file-size-bytes selects files > maxThreshold (the 1 large file)
            // Medium files should be skipped (not too small, not too large)
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['min-input-files', 'min-file-size-bytes', 'max-file-size-bytes'], array['1', '%d', '%d']))", TEST_SCHEMA, tableName, minThreshold, maxThreshold), 17);

            table.refresh();
            // 2 small files + 1 large file -> 1 combined file
            // 2 medium files remain unchanged
            // Total: 3 files
            assertHasDataFiles(table.currentSnapshot(), 3);

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'a'), (2, 'b'), (3, 'data'), (4, 'data'), (5, 'data'), " +
                    "(6, 'data'), (7, 'data'), (8, 'data'), (9, 'data'), (10, 'data'), " +
                    "(11, 'data'), (12, 'data'), (13, 'data'), (14, 'data'), (15, 'data'), " +
                    "(16, 'data'), (17, 'data'), (18, 'data'), (19, 'data'), (20, 'data'), " +
                    "(21, 'data'), (22, 'data'), (23, 'data'), (24, 'data'), (25, 'data'), " +
                    "(26, 'data'), (27, 'data')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithMinInputFilesAndFileSizeOptions()
    {
        String tableName = "test_min_input_files_and_file_size";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar) WITH (partitioning = ARRAY['category'])");

            // Partition 'A': 3 small files + 1 large file (meets min-input-files>=3 and has small files)
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'A')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'A')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(4, 'A'), (5, 'A'), (6, 'A'), (7, 'A'), (8, 'A')", 5);
            // Partition 'B': 2 small files (has small files but doesn't meet min-input-files>=3)
            assertUpdate("INSERT INTO " + tableName + " VALUES (9, 'B')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (10, 'B')", 1);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 6);

            // Get file sizes to set threshold that selects small files
            List<Long> allFileSizes = new ArrayList<>();
            for (FileScanTask task : table.newScan().planFiles()) {
                allFileSizes.add(task.file().fileSizeInBytes());
            }
            Collections.sort(allFileSizes);
            // allFileSizes should be: [small_A, small_A, small_A, small_B, small_B, large_A]
            // Set threshold between small and large files
            long threshold = (allFileSizes.get(4) + allFileSizes.get(5)) / 2;

            // Combine min-input-files=3 with min-file-size-bytes
            // File size filter selects: small files from A (3) and small files from B (2)
            // Group filter then selects: only files from partitions with >= 3 files (partition A only)
            // Result: Only partition A's 3 small files are rewritten
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['min-input-files', 'min-file-size-bytes'], array['3', '%d']))", TEST_SCHEMA, tableName, threshold), 3);

            table.refresh();
            // Partition 'A': 3 small files -> 1 file, large file unchanged
            // Partition 'B': 2 small files unchanged (doesn't meet min-input-files>=3)
            // Total: 4 files
            assertHasDataFiles(table.currentSnapshot(), 4);

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A'), (2, 'A'), (3, 'A'), (4, 'A'), (5, 'A'), (6, 'A'), (7, 'A'), (8, 'A'), (9, 'B'), (10, 'B')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithAllOptionsCombined()
    {
        String tableName = "test_all_options_combined";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar) WITH (partitioning = ARRAY['category'])");

            // Create multiple files with varying sizes in different partitions
            // Partition 'A': 3 small files + 1 medium file + 1 very large file (meets min-input-files>=3)
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'A')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'A')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(4, 'A'), (5, 'A'), (6, 'A'), (7, 'A'), (8, 'A')", 5);
            // Very large file with many rows
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(9, 'A'), (10, 'A'), (11, 'A'), (12, 'A'), (13, 'A'), " +
                    "(14, 'A'), (15, 'A'), (16, 'A'), (17, 'A'), (18, 'A'), " +
                    "(19, 'A'), (20, 'A'), (21, 'A'), (22, 'A'), (23, 'A')", 15);
            // Partition 'B': 2 small files (doesn't meet min-input-files>=3)
            assertUpdate("INSERT INTO " + tableName + " VALUES (24, 'B')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (25, 'B')", 1);
            // Partition 'C': 1 file (not in filter)
            assertUpdate("INSERT INTO " + tableName + " VALUES (26, 'C')", 1);

            // Add deletes
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 25", 1);

            Table table = loadTable(tableName);
            assertHasDataFiles(table.currentSnapshot(), 8);
            assertHasDeleteFiles(table.currentSnapshot(), 2);

            // Dynamically determine file size thresholds based on actual file sizes
            List<Long> partitionAFileSizes = new ArrayList<>();
            for (FileScanTask task : table.newScan().planFiles()) {
                String partition = task.file().partition().get(0, String.class);
                if (partition.equals("A")) {
                    partitionAFileSizes.add(task.file().fileSizeInBytes());
                }
            }
            Collections.sort(partitionAFileSizes);

            // Set thresholds to select 3 smallest files and 1 largest file
            long minThreshold = partitionAFileSizes.get(2) + 1;
            long maxThreshold = partitionAFileSizes.get(4) - 1;

            // Rewrite with all options: filter, min-input-files, min-file-size-bytes, max-file-size-bytes
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', filter => 'category IN (''A'', ''B'')', options => map(array['min-input-files', 'min-file-size-bytes', 'max-file-size-bytes'], array['3', '%d', '%d']))",
                    TEST_SCHEMA, tableName, minThreshold, maxThreshold), 17);

            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 5);
            assertHasDeleteFiles(table.currentSnapshot(), 1);

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A'), (3, 'A'), (4, 'A'), (5, 'A'), (6, 'A'), (7, 'A'), (8, 'A'), " +
                    "(9, 'A'), (10, 'A'), (11, 'A'), (12, 'A'), (13, 'A'), (14, 'A'), (15, 'A'), " +
                    "(16, 'A'), (17, 'A'), (18, 'A'), (19, 'A'), (20, 'A'), (21, 'A'), (22, 'A'), (23, 'A'), " +
                    "(24, 'B'), (26, 'C')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteAllOption()
    {
        String tableName = "test_rewrite_all_option";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, category varchar, value varchar) WITH (partitioning = ARRAY['category'])");

            // Create files with varying sizes across partitions
            // Partition A: 3 small files
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 'val1')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'A', 'val2')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'A', 'val3')", 1);

            // Partition B: 2 files
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'B', 'val4')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (5, 'B', 'val5')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 5);

            // Without rewrite-all, min-input-files=4 would skip all partitions (A has 3, B has 2)
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['min-input-files'], array['4']))",
                    TEST_SCHEMA, tableName), 0);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 5); // No change

            // With rewrite-all=true, all files should be rewritten regardless of min-input-files
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))",
                    TEST_SCHEMA, tableName), 5);
            table.refresh();
            assertHasDataFiles(table.currentSnapshot(), 2); // 1 file per partition

            // Verify data integrity
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 'val1'), (2, 'A', 'val2'), (3, 'A', 'val3'), (4, 'B', 'val4'), (5, 'B', 'val5')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteAllOverridesFileFilters()
    {
        String tableName1 = "test_rewrite_all_min_filter";
        String tableName2 = "test_rewrite_all_max_filter";
        String tableName3 = "test_rewrite_all_no_filter";

        try {
            // Test 1: Use only min-file-size-bytes with a very high threshold
            // This should select files that are SMALLER than the threshold (which is all)
            assertUpdate("CREATE TABLE " + tableName1 + " (id integer, value varchar)");
            assertUpdate("INSERT INTO " + tableName1 + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName1 + " VALUES (2, 'b')", 1);
            assertUpdate("INSERT INTO " + tableName1 + " VALUES (3, 'c')", 1);

            Table table1 = loadTable(tableName1);
            table1.refresh();
            assertHasDataFiles(table1.currentSnapshot(), 3);

            long veryLargeThreshold = 999999999L;
            // Set min-input-files=1 since we only have 3 files
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['min-input-files', 'min-file-size-bytes'], array['1', '%d']))", TEST_SCHEMA, tableName1, veryLargeThreshold), 3); // All files are smaller than threshold
            table1.refresh();
            assertHasDataFiles(table1.currentSnapshot(), 1);
            assertQuery("SELECT * FROM " + tableName1 + " ORDER BY id", "VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            // Test 2: Use only max-file-size-bytes with a very small threshold
            // This should select files that are LARGER than the threshold (which is all)
            assertUpdate("CREATE TABLE " + tableName2 + " (id integer, value varchar)");
            assertUpdate("INSERT INTO " + tableName2 + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName2 + " VALUES (2, 'b')", 1);
            assertUpdate("INSERT INTO " + tableName2 + " VALUES (3, 'c')", 1);

            Table table2 = loadTable(tableName2);
            table2.refresh();
            assertHasDataFiles(table2.currentSnapshot(), 3);

            long verySmallThreshold = 1L;
            // Set min-input-files=1 since we only have 3 files
            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['min-input-files', 'max-file-size-bytes'], array['1', '%d']))", TEST_SCHEMA, tableName2, verySmallThreshold), 3); // All files are larger than threshold
            table2.refresh();
            assertHasDataFiles(table2.currentSnapshot(), 1);
            assertQuery("SELECT * FROM " + tableName2 + " ORDER BY id", "VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            // Test 3: With rewrite-all=true, ignore the filters entirely
            assertUpdate("CREATE TABLE " + tableName3 + " (id integer, value varchar)");
            assertUpdate("INSERT INTO " + tableName3 + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName3 + " VALUES (2, 'b')", 1);
            assertUpdate("INSERT INTO " + tableName3 + " VALUES (3, 'c')", 1);

            Table table3 = loadTable(tableName3);
            table3.refresh();
            assertHasDataFiles(table3.currentSnapshot(), 3);

            assertUpdate(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))",
                    TEST_SCHEMA, tableName3), 3);
            table3.refresh();
            assertHasDataFiles(table3.currentSnapshot(), 1);
            assertQuery("SELECT * FROM " + tableName3 + " ORDER BY id", "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
        finally {
            dropTable(tableName1);
            dropTable(tableName2);
            dropTable(tableName3);
        }
    }

    private Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }

    private void assertHasSize(Iterable iterable, int size)
    {
        AtomicInteger count = new AtomicInteger(0);
        iterable.forEach(obj -> count.incrementAndGet());
        assertEquals(count.get(), size);
    }

    private void assertHasDataFiles(Snapshot snapshot, int dataFilesCount)
    {
        Map<String, String> map = snapshot.summary();
        int totalDataFiles = Integer.valueOf(map.get(TOTAL_DATA_FILES_PROP));
        assertEquals(totalDataFiles, dataFilesCount);
    }

    private void assertHasDeleteFiles(Snapshot snapshot, int deleteFilesCount)
    {
        Map<String, String> map = snapshot.summary();
        int totalDeleteFiles = Integer.valueOf(map.get(TOTAL_DELETE_FILES_PROP));
        assertEquals(totalDeleteFiles, deleteFilesCount);
    }

    private void assertFilesPlan(CloseableIterator<FileScanTask> iterator, int dataFileCount, int deleteFileCount)
    {
        AtomicInteger dataCount = new AtomicInteger(0);
        AtomicInteger deleteCount = new AtomicInteger(0);
        while (iterator.hasNext()) {
            FileScanTask fileScanTask = iterator.next();
            dataCount.incrementAndGet();
            deleteCount.addAndGet(fileScanTask.deletes().size());
        }
        assertEquals(dataCount.get(), dataFileCount);
        assertEquals(deleteCount.get(), deleteFileCount);

        try {
            iterator.close();
            iterator = CloseableIterator.empty();
        }
        catch (Exception e) {
            // do nothing
        }
    }
}
