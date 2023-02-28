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
package com.facebook.presto.hive.s3select;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveFileSystemTestUtils.filterTable;
import static com.facebook.presto.hive.HiveFileSystemTestUtils.newSession;
import static com.facebook.presto.hive.HiveFileSystemTestUtils.readTable;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;

public class TestHiveFileSystemS3SelectJsonPushdown
{
    private S3SelectTestHelper s3SelectTestHelper;
    private SchemaTableName jsonTable;

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
        s3SelectTestHelper = new S3SelectTestHelper(host, port, databaseName, awsAccessKey, awsSecretKey, writableBucket);
        jsonTable = new SchemaTableName(databaseName, "presto_test_external_fs_json");
    }

    @Test
    public void testJsonGetRecords()
            throws Exception
    {
        assertEqualsIgnoreOrder(
                readTable(jsonTable,
                        s3SelectTestHelper.getTransactionManager(),
                        s3SelectTestHelper.getConfig(),
                        s3SelectTestHelper.getMetadataFactory(),
                        s3SelectTestHelper.getPageSourceProvider(),
                        s3SelectTestHelper.getSplitManager()),
                MaterializedResult.resultBuilder(newSession(s3SelectTestHelper.getConfig()), BIGINT, BIGINT)
                        .row(2L, 4L).row(3L, 5L) // test_table.json
                        .row(7L, 23L).row(28L, 22L).row(13L, 10L) // test_table.json.gz
                        .row(1L, 19L).row(6L, 3L).row(24L, 22L).row(100L, 77L) // test_table.json.bz2
                        .build());
    }

    @Test
    public void testJsonFilterRecords()
            throws Exception
    {
        List<ColumnHandle> projectedColumns = ImmutableList.of(
                new HiveColumnHandle("col_1", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty()));

        assertEqualsIgnoreOrder(
                filterTable(jsonTable,
                        projectedColumns,
                        s3SelectTestHelper.getTransactionManager(),
                        s3SelectTestHelper.getConfig(),
                        s3SelectTestHelper.getMetadataFactory(),
                        s3SelectTestHelper.getPageSourceProvider(),
                        s3SelectTestHelper.getSplitManager()),
                MaterializedResult.resultBuilder(newSession(s3SelectTestHelper.getConfig()), BIGINT, BIGINT)
                        .row(2L).row(3L) // test_table.json
                        .row(7L).row(28L).row(13L) // test_table.json.gz
                        .row(1L).row(6L).row(24L).row(100L) // test_table.json.bz2
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        s3SelectTestHelper.tearDown();
    }
}
