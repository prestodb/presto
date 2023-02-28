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

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import io.airlift.units.DataSize;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveFileSystemTestUtils.newSession;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.s3select.S3SelectTestHelper.expectedResult;
import static com.facebook.presto.hive.s3select.S3SelectTestHelper.isSplitCountInOpenInterval;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static org.testng.Assert.assertTrue;

public class TestHiveFileSystemS3SelectCsvPushdownWithSplits
{
    private String host;
    private int port;
    private String databaseName;
    private String awsAccessKey;
    private String awsSecretKey;
    private String writableBucket;

    private SchemaTableName csvTableWithSplits;

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
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.writableBucket = writableBucket;
        csvTableWithSplits = new SchemaTableName(databaseName, "presto_test_csv_scan_range_select_pushdown");
    }

    @DataProvider(name = "testSplitSize")
    public static Object[][] splitSizeParametersProvider()
    {
        return new Object[][] {{3, 2, 15, 30}, {50, 30, 2, 4}};
    }

    @Test(dataProvider = "testSplitSize")
    public void testQueryPushdownWithSplitSizeForJson(int maxSplitSizeKB,
            int maxInitialSplitSizeKB,
            int minSplitCount,
            int maxSplitCount)
    {
        HiveClientConfig hiveConfig = new HiveClientConfig()
                    .setS3SelectPushdownEnabled(true)
                    .setMaxSplitSize(new DataSize(maxSplitSizeKB, KILOBYTE))
                    .setMaxInitialSplitSize(new DataSize(maxInitialSplitSizeKB, KILOBYTE));

        try (S3SelectTestHelper s3SelectTestHelper = new S3SelectTestHelper(
                    host,
                    port,
                    databaseName,
                    awsAccessKey,
                    awsSecretKey,
                    writableBucket,
                    hiveConfig)) {
            int tableSplitsCount = s3SelectTestHelper.getTableSplitsCount(csvTableWithSplits);
            assertTrue(isSplitCountInOpenInterval(tableSplitsCount, minSplitCount, maxSplitCount));

            HiveColumnHandle indexColumn = new HiveColumnHandle("index", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty());
            MaterializedResult filteredTableResult = s3SelectTestHelper.getFilteredTableResult(csvTableWithSplits, indexColumn);
            assertEqualsIgnoreOrder(filteredTableResult,
                    expectedResult(newSession(s3SelectTestHelper.getConfig()), 1, 300));
        }
    }
}
