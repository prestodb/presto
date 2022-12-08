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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;

public class TestHiveFileSystemS3SelectPushdown
        extends AbstractTestHiveFileSystemS3
{
    protected SchemaTableName commaDelimitedTable;
    protected SchemaTableName pipeDelimitedTable;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.s3.awsAccessKey",
            "hive.hadoop2.s3.awsSecretKey",
            "hive.hadoop2.s3.writableBucket"})
    @BeforeClass
    public void setup(String host, int port, String databaseName, String awsAccessKey, String awsSecretKey, String writableBucket)
    {
        super.setup(host, port, databaseName, awsAccessKey, awsSecretKey, writableBucket, true);
        commaDelimitedTable = new SchemaTableName(database, "presto_test_external_fs_comma_delimited");
        pipeDelimitedTable = new SchemaTableName(database, "presto_test_external_fs_pipe_delimited");
    }

    @Test
    public void testGetCommaDelimitedRecords()
            throws Exception
    {
        assertEqualsIgnoreOrder(
                readTable(commaDelimitedTable),
                MaterializedResult.resultBuilder(newSession(), BIGINT, BIGINT)
                        .row(7L, 1L).row(19L, 10L).row(1L, 345L)                // test_comma_delimited_table.csv
                        .row(27L, 10L).row(28L, 9L).row(90L, 94L)               // test_comma_delimited_table.csv.gzip
                        .row(11L, 24L).row(1L, 6L).row(21L, 12L).row(0L, 0L)    // test_comma_delimited_table.csv.bz2
                        .build());
    }

    @Test
    public void testFilterCommaDelimitedRecords()
            throws Exception
    {
        List<ColumnHandle> projectedColumns = ImmutableList.of(
                new HiveColumnHandle("t_bigint", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty()));

        assertEqualsIgnoreOrder(
                filterTable(commaDelimitedTable, projectedColumns),
                MaterializedResult.resultBuilder(newSession(), BIGINT)
                        .row(7L).row(19L).row(1L)             // test_comma_delimited_table.csv
                        .row(27L).row(28L).row(90L)           // test_comma_delimited_table.csv.gzip
                        .row(11L).row(1L).row(21L).row(0L)    // test_comma_delimited_table.csv.bz2
                        .build());
    }

    @Test
    public void testGetPipeDelimitedRecords()
            throws Exception
    {
        assertEqualsIgnoreOrder(
                readTable(pipeDelimitedTable),
                MaterializedResult.resultBuilder(newSession(), BIGINT, BIGINT)
                        .row(1L, 1L).row(2L, 2L).row(3L, 3L)        // test_pipe_delimited_table.csv
                        .row(10L, 12L).row(7L, 8L).row(392L, 166L)  // test_pipe_delimited_table.csv.gzip
                        .row(10L, 20L).row(30L, 40L).row(50L, 60L)  // test_pipe_delimited_table.csv.bz2
                        .build());
    }

    @Test
    public void testFilterPipeDelimitedRecords()
            throws Exception
    {
        List<ColumnHandle> projectedColumns = ImmutableList.of(
                new HiveColumnHandle("t_bigint", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty()));

        assertEqualsIgnoreOrder(
                filterTable(pipeDelimitedTable, projectedColumns),
                MaterializedResult.resultBuilder(newSession(), BIGINT)
                        .row(1L).row(2L).row(3L)        // test_pipe_delimited_table.csv
                        .row(10L).row(7L).row(392L)     // test_pipe_delimited_table.csv.gzip
                        .row(10L).row(30L).row(50L)     // test_pipe_delimited_table.csv.bz2
                        .build());
    }
}
