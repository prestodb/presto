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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestRewriteDataFilesProcedure
        extends AbstractTestQueryFramework
{
    @Test
    public void testRewriteDataFilesOnTableWithNotNullColumn()
    {
        String tableName = "example_not_null_column_table";
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar NOT NULL)");

            // create 3 files
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, 'foo'), (4, 'bar')", 2);
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo'), (6, 'bar')", 2);
            validateDataFilesAndDeleteFiles(tableName, 3L, 0L);

            // The procedure writes through a CallDistributedProcedureNode, which carries the
            // table's NOT NULL columns like any other write. Rows already satisfy the constraint,
            // so rewriting them keeps every row.
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 6);

            validateDataFilesAndDeleteFiles(tableName, 1L, 0L);
            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (2, 'bar'), (3, 'foo'), (4, 'bar'), (5, 'foo'), (6, 'bar')");
        }
        finally {
            dropTable(tableName);
        }
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

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 7);

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
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => '1 = 1', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 10);

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

            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 7);

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
            assertUpdate(format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 = ''bar''', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 5);
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
    public void testRewriteDataFilesWithFilterPushdownEnabled()
    {
        String tableName = "example_partition_filter_pushdown_table";
        String schemaName = getSession().getSchema().get();
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) with (partitioning = ARRAY['c2'])");

            // create 1 files for each partition (c2 = 'foo' or 'bar')
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'foo'), (3, 'foo'), (4, 'foo'), (5, 'foo')", 5);
            assertUpdate("INSERT INTO " + tableName + " values(1, 'bar'), (2, 'bar'), (3, 'bar'), (4, 'bar'), (5, 'bar')", 5);

            //The number of data files is 2, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 2L, 0L);

            // Non-partition column filter: blocked (c1 is in domainPredicate as non-partition).
            assertQueryFails(sessionWithFilterPushdown, format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");

            // IN list on non-partition column: blocked (same domainPredicate path as range).
            assertQueryFails(sessionWithFilterPushdown, format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 IN (1, 2, 3)')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");

            // Combined partition + non-partition (AND): blocked because c1 is non-partition.
            // Verifies that a partition predicate does not whitelist the whole filter.
            assertQueryFails(sessionWithFilterPushdown, format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 = ''bar'' AND c1 > 3')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");

            // OR across columns: cannot be simplified to a domain; lands in remainingPredicate.
            assertQueryFails(sessionWithFilterPushdown, format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3 OR c2 = ''bar''')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");

            // Partition column filter with pushdown enabled: safe to proceed. Files are pruned by
            // partition boundary and all rows in selected files are preserved.
            assertUpdate(sessionWithFilterPushdown, format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 = ''bar''')", tableName, schemaName), 0);
            assertUpdate(sessionWithFilterPushdown, format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s')", tableName, schemaName), 0);

            // Partition column filter with pushdown enabled and rewrite-all: rewrites the bar
            // partition and all 5 rows are preserved (no data loss).
            assertUpdate(sessionWithFilterPushdown, format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 = ''bar''', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 5);
            validateDataFilesAndDeleteFiles(tableName, 2L, 0L);

            assertQuery("select * from " + tableName,
                    "values(1, 'foo'), (1, 'bar'), " +
                            "(2, 'foo'), (2, 'bar'), " +
                            "(3, 'foo'), (3, 'bar'), " +
                            "(4, 'foo'), (4, 'bar'), " +
                            "(5, 'foo'), (5, 'bar')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithFunctionFilterAndPushdownEnabled()
    {
        // Covers the remainingPredicate path: function expressions like lower(c2)='bar' cannot be
        // expressed as domain predicates and are pushed to Velox as row-level filters, causing
        // data loss. The guard must check remainingPredicate, not just domainPredicate.
        String tableName = "example_function_filter_pushdown_table";
        String schemaName = getSession().getSchema().get();
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) with (partitioning = ARRAY['c2'])");
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'foo'), (3, 'foo')", 3);
            assertUpdate("INSERT INTO " + tableName + " values(1, 'bar'), (2, 'bar'), (3, 'bar')", 3);

            assertQueryFails(sessionWithFilterPushdown,
                    format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'lower(c2) = ''bar''')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithSubfieldFilterAndPushdownEnabled()
    {
        // Covers the domainPredicate subfield path: predicates like person.age > 25 are stored
        // as non-entire-column Subfield entries in domainPredicate. getValidPredicate() silently
        // drops them (isEntireColumn check), so the guard must inspect domainPredicate directly.
        String tableName = "example_subfield_filter_pushdown_table";
        String schemaName = getSession().getSchema().get();
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar, person ROW(name VARCHAR, age INTEGER)) with (partitioning = ARRAY['c2'])");
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo', row('Alice', 30)), (2, 'foo', row('Bob', 20))", 2);
            assertUpdate("INSERT INTO " + tableName + " values(1, 'bar', row('Charlie', 25)), (2, 'bar', row('Dave', 35))", 2);

            assertQueryFails(sessionWithFilterPushdown,
                    format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'person.age > 25')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithNonIdentityPartitionFilterAndPushdownEnabled()
    {
        // getPartitionKeyColumnHandles() only includes identity-transform partition columns.
        // For bucket(c1, 4), c1 is NOT in partitionColumns, so the guard treats c1 as a
        // non-partition column and blocks the filter — preventing data loss where Velox would
        // apply c1=5 as a row filter on files that also contain c1=1,9,13 (same bucket).
        String tableName = "example_bucket_partition_pushdown_table";
        String schemaName = getSession().getSchema().get();
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) with (partitioning = ARRAY['bucket(c1, 4)'])");
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (5, 'bar'), (9, 'baz')", 3);

            assertQueryFails(sessionWithFilterPushdown,
                    format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 = 5')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithNullPartitionFilterAndPushdownEnabled()
    {
        // IS NULL on an identity partition column is safe: all rows in the NULL-partition files
        // have c2 = NULL, so Velox applying c2 IS NULL as a row filter drops nothing.
        // The guard must allow this — c2 is in partitionColumnNames.
        String tableName = "example_null_partition_pushdown_table";
        String schemaName = getSession().getSchema().get();
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) with (partitioning = ARRAY['c2'])");
            // Two files in the NULL partition
            assertUpdate("INSERT INTO " + tableName + " values(1, NULL), (2, NULL)", 2);
            assertUpdate("INSERT INTO " + tableName + " values(3, NULL), (4, NULL)", 2);
            // One file in a non-null partition to confirm data isolation
            assertUpdate("INSERT INTO " + tableName + " values(5, 'foo')", 1);

            // Guard passes (c2 is an identity partition column); rewrite-all with pushdown
            // enabled rewrites the 2-file NULL partition and Prestissimo preserves all rows.
            assertUpdate(sessionWithFilterPushdown,
                    format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 IS NULL', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 4);
            validateDataFilesAndDeleteFiles(tableName, 2L, 0L);

            assertQuery("select * from " + tableName,
                    "values(1, NULL), (2, NULL), (3, NULL), (4, NULL), (5, 'foo')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithDateBasedPartitionFilterAndPushdownEnabled()
    {
        // year(event_date) is a non-identity transform. getPartitionKeyColumnHandles() only
        // includes identity transforms, so event_date is NOT in partitionColumns. The guard
        // treats event_date as a non-partition column and blocks the filter, preventing Velox
        // from silently dropping rows whose event_date does not match the predicate.
        String tableName = "example_year_partition_pushdown_table";
        String schemaName = getSession().getSchema().get();
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, event_date date) with (partitioning = ARRAY['year(event_date)'])");
            assertUpdate("INSERT INTO " + tableName + " values(1, DATE '2023-06-15'), (2, DATE '2024-03-20'), (3, DATE '2024-11-05')", 3);

            assertQueryFails(sessionWithFilterPushdown,
                    format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'event_date >= DATE ''2024-01-01''')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithMultipleIdentityPartitionColumnsAndPushdownEnabled()
    {
        // Verifies that filtering on ALL identity partition columns is allowed when pushdown is
        // enabled. Every column in the filter is in partitionColumns, so no non-partition
        // predicate is present and the guard does not fire.
        String tableName = "example_multi_partition_pushdown_table";
        String schemaName = getSession().getSchema().get();
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar, c3 varchar) with (partitioning = ARRAY['c2', 'c3'])");
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo', 'x'), (2, 'foo', 'x'), (3, 'foo', 'x')", 3);
            assertUpdate("INSERT INTO " + tableName + " values(1, 'bar', 'y'), (2, 'bar', 'y'), (3, 'bar', 'y')", 3);

            // Filter on all partition columns: allowed, data is preserved.
            assertUpdate(sessionWithFilterPushdown,
                    format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c2 = ''bar'' AND c3 = ''y''', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 3);
            validateDataFilesAndDeleteFiles(tableName, 2L, 0L);

            assertQuery("select * from " + tableName,
                    "values(1, 'foo', 'x'), (2, 'foo', 'x'), (3, 'foo', 'x'), " +
                            "(1, 'bar', 'y'), (2, 'bar', 'y'), (3, 'bar', 'y')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteDataFilesWithFilterOnUnpartitionedTableAndPushdownEnabled()
    {
        // On a table with no partition columns, partitionColumnNames is empty, so any filter
        // on any column is treated as a non-partition predicate and blocked.
        String tableName = "example_unpartitioned_pushdown_table";
        String schemaName = getSession().getSchema().get();
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");
            assertUpdate("INSERT INTO " + tableName + " values(1, 'foo'), (2, 'bar'), (3, 'baz')", 3);
            assertUpdate("INSERT INTO " + tableName + " values(4, 'foo'), (5, 'bar'), (6, 'baz')", 3);

            assertQueryFails(sessionWithFilterPushdown,
                    format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c1 > 3')", tableName, schemaName),
                    "rewrite_data_files with a non-partition column filter is not supported when pushdown_filter_enabled=true");

            // No filter on an unpartitioned table: safe regardless of pushdown setting.
            assertUpdate(sessionWithFilterPushdown,
                    format("CALL system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 6);
            validateDataFilesAndDeleteFiles(tableName, 1L, 0L);
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

            assertUpdate(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 3);
            //The number of data files is 3, and the number of delete files is 0
            validateDataFilesAndDeleteFiles(tableName, 3L, 0L);
            assertQuery("select * from " + tableName, "values(2, '1002', NULL), (5, '1005', 5), (7, '1007', 7)");

            result = getExpectedQueryRunner().execute(getSession(), "DELETE from " + tableName + " WHERE b = '1002'", ImmutableList.of(BigintType.BIGINT));
            assertEquals(result.getOnlyValue(), 1L);
            //The number of data files is 3, and the number of delete files is 1
            validateDataFilesAndDeleteFiles(tableName, 3L, 1L);
            assertUpdate(format("call system.rewrite_data_files(table_name => '%s', schema => '%s', filter => 'c is null', options => map(array['rewrite-all'], array['true']))", tableName, schemaName), 0);

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
        validateDataFilesAndDeleteFiles(tableName, 10L, 0L);
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
        validateDataFilesAndDeleteFiles(tableName, 5L, 0L);
    }

    private Session pushdownFilterEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
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
