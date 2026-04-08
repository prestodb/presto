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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestRewriteDataFilesProcedure
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Test
    public void testRewriteDataFilesInEmptyTable()
    {
        String tableName = "default_empty_table";
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
            assertUpdate(format("CALL system.rewrite_data_files('%s', '%s')", schemaName, tableName), 0);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesOnNonPartitionTable()
    {
        String tableName = "example_non_partition_table";
        String schemaName = getSession().getSchema().get();
        try {
            createNonPartitionedTableWithInitialDataAndValidate(tableName);

            MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE c1 = 7", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE c1 in (9, 10)", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 2L);

            //The number of data files is 5, and the number of delete files is 2
            validateDataFilesAndDeleteFiles(tableName, 5L, 2L);
            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(8, 'bar')");

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, schemaName), 7);

            //The number of data files is 1, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 1L, 0L);

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
    public void testRewriteDataFilesWithDeterministicTrueFilter()
    {
        String tableName = "example_non_partition_true_filter_table";
        String schemaName = getSession().getSchema().get();
        try {
            createNonPartitionedTableWithInitialDataAndValidate(tableName);

            // Does not support rewriting files filtered by non-partitioned columns with couldn't be pushed down thoroughly
            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3')", tableName, schemaName),
                    ".* probably connector was not able to handle provided WHERE expression");

            // the filter is `true` means select all files to rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 = 1')", tableName, schemaName), 10);

            //The number of data files is 1, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 1L, 0L);

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
        String schemaName = getSession().getSchema().get();
        try {
            createNonPartitionedTableWithInitialDataAndValidate(tableName);

            // the filter is `false` means select no file to rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 = 0')", tableName, schemaName), 0);

            //The number of data files is still 5, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 5L, 0L);

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
    public void testInvalidParameterCases()
    {
        String tableName = "invalid_parameter_table";
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (a int, b varchar, c int)");
            assertQueryFails("CALL system.rewrite_data_files('n', table_name => 't')", ".*Named and positional arguments cannot be mixed");
            assertQueryFails("CALL custom.rewrite_data_files('n', 't')", "Procedure not registered: custom.rewrite_data_files");
            assertQueryFails("CALL system.rewrite_data_files()", ".*Required procedure argument 'schema' is missing");
            assertQueryFails("CALL system.rewrite_data_files('s', 'n')", "Schema s does not exist");
            assertQueryFails("CALL system.rewrite_data_files('', '')", "Table name is empty");
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '''hello''')", tableName, schemaName), ".*WHERE clause must evaluate to a boolean: actual type varchar\\(5\\)");
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1001')", tableName, schemaName), ".*WHERE clause must evaluate to a boolean: actual type integer");
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'a')", tableName, schemaName), ".*WHERE clause must evaluate to a boolean: actual type integer");
            assertQueryFails(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'n')", tableName, schemaName), ".*Column 'n' cannot be resolved");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesOnPartitionTable()
    {
        String tableName = "example_partition_table";
        String schemaName = getSession().getSchema().get();
        try {
            createPartitionedTableWithInitialDataAndValidate(tableName);

            MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE c1 = 7", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE c1 in (8, 10)", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 2L);

            //The number of data files is 10, and the number of delete files is 3
            validateDataFilesAndDeleteFiles(tableName, 10L, 3L);
            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(9, 'foo')");

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, schemaName), 7);

            //The number of data files is 2, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 2L, 0L);
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
    public void testRewriteDataFilesWithFilter()
    {
        String tableName = "example_partition_filter_table";
        String schemaName = getSession().getSchema().get();
        try {
            createPartitionedTableWithInitialDataAndValidate(tableName);

            // Does not support rewriting files filtered by non-partitioned columns with couldn't be pushed down thoroughly
            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3')", tableName, schemaName),
                    ".* probably connector was not able to handle provided WHERE expression");

            // select 5 files to rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 = ''bar''')", tableName, schemaName), 5);
            //The number of data files is 6, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 6L, 0L);

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
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (a int, b varchar)");
            assertUpdate("INSERT INTO " + tableName + " values(1, '1001'), (2, '1002')", 2);
            MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE a = 1", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            assertQuery("select * from " + tableName, "values(2, '1002')");

            //The number of data files is 1, and the number of delete files is 1
            validateDataFilesAndDeleteFiles(tableName, 1L, 1L);

            assertUpdate("alter table " + tableName + " add column c int with (partitioning = 'identity')");
            assertUpdate("INSERT INTO " + tableName + " values(5, '1005', 5), (6, '1006', 6), (7, '1007', 7)", 3);
            result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE b = '1006'", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            assertQuery("select * from " + tableName, "values(2, '1002', NULL), (5, '1005', 5), (7, '1007', 7)");

            //The number of data files is 4, and the number of delete files is 2
            validateDataFilesAndDeleteFiles(tableName, 4L, 2L);

            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'a > 3')", tableName, schemaName),
                    ".* probably connector was not able to handle provided WHERE expression");
            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c > 3')", tableName, schemaName),
                    ".* probably connector was not able to handle provided WHERE expression");

            assertUpdate(format("call system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, schemaName), 3);
            //The number of data files is 3, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 3L, 0L);
            assertQuery("select * from " + tableName, "values(2, '1002', NULL), (5, '1005', 5), (7, '1007', 7)");

            result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE b = '1002'", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            //The number of data files is 3, and the number of delete files is 1
            validateDataFilesAndDeleteFiles(tableName, 3L, 1L);
            assertUpdate(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c is null')", tableName, schemaName), 0);

            //The number of data files is 2, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 2L, 0L);
            assertQuery("select * from " + tableName, "values(5, '1005', 5), (7, '1007', 7)");

            // This is a metadata delete
            result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE c = 7", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            //The number of data files is 1, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 1L, 0L);
            assertQuery("select * from " + tableName, "values(5, '1005', 5)");
        }
        finally {
            dropTable(tableName);
        }
    }

    private void createPartitionedTableWithInitialDataAndValidate(String tableName)
    {
        assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) with (partitioning = ARRAY['c2'])");

        // create 5 files for each partition (c2 = 'foo' or 'bar')
        assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
        assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
        assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
        assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
        assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

        //The number of data files is 10, and the number of delete files is 0
        MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
        assertEquals(result.getOnlyValue(), 10L);
        result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
        assertEquals(result.getOnlyValue(), 0L);
    }

    private void createNonPartitionedTableWithInitialDataAndValidate(String tableName)
    {
        assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");

        // create 5 files
        assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
        assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
        assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
        assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
        assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

        //The number of data files is 5, and the number of delete files is 0
        MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
        assertEquals(result.getOnlyValue(), 5L);
        result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
        assertEquals(result.getOnlyValue(), 0L);
    }

    private void validateDataFilesAndDeleteFiles(String tableName, long dataFiles, long deleteFiles)
    {
        MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
        assertEquals(result.getOnlyValue(), dataFiles);
        result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
        assertEquals(result.getOnlyValue(), deleteFiles);
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + tableName);
    }
}
