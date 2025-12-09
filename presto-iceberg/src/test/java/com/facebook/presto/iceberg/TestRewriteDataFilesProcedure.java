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

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, TEST_SCHEMA), 7);

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

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, TEST_SCHEMA), 7);

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
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 = ''bar''')", tableName, TEST_SCHEMA), 5);
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
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 = 1')", tableName, TEST_SCHEMA), 10);

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

            assertUpdate(format("call system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, TEST_SCHEMA), 3);
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
            assertUpdate(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c is null')", tableName, TEST_SCHEMA), 0);

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
