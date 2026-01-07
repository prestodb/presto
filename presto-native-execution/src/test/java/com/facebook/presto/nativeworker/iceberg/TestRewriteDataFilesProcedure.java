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
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");

            // create 5 files
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

            //The number of data files is 5，and the number of delete files is 0
            MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 5L);
            result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 0L);

            result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE c1 = 7", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE c1 in (9, 10)", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 2L);

            //The number of data files is 5，and the number of delete files is 2
            result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 5L);
            result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 2L);
            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (6, 'bar'), " +
                            "(8, 'bar')");

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, schemaName), 7);

            //The number of data files is 1，and the number of delete files is 0
            result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 0L);

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
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");

            // create 5 files
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

            //The number of data files is 5，and the number of delete files is 0
            MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 5L);
            result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 0L);

            // do not support rewrite files filtered by non-identity columns
            assertQueryFails(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3')", tableName, schemaName), ".*");

            // the filter is `true` means select all files to rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 = 1')", tableName, schemaName), 10);

            //The number of data files is 1，and the number of delete files is 0
            result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 0L);

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
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");

            // create 5 files
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(7, 'foo'), (8, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(9, 'foo'), (10, 'bar')", 2);

            //The number of data files is 5，and the number of delete files is 0
            MaterializedResult result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 5L);
            result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 0L);

            // the filter is `false` means select no file to rewrite
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 = 0')", tableName, schemaName), 0);

            //The number of data files is still 5，and the number of delete files is 0
            result = getExpectedQueryRunner().execute(getSession(), "select count(*) from \"" + tableName + "$files\"", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 5L);
            result = getExpectedQueryRunner().execute(getSession(), "select count(distinct \"$delete_file_path\") from " + tableName, ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 0L);

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
        // Not support write to partitioned table yet
    }

    @Test
    public void testRewriteDataFilesWithFilter()
    {
        // Not support write to partitioned table yet
    }

    @Test
    public void testRewriteDataFilesWithDeleteAndPartitionEvolution()
    {
        // Not support write to partitioned table yet
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + tableName);
    }
}
