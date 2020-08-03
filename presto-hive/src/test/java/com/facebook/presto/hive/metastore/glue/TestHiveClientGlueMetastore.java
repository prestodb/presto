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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.hive.AbstractTestHiveClientLocal;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.google.common.collect.Sets.difference;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveClientGlueMetastore
        extends AbstractTestHiveClientLocal
{
    private ExecutorService executorService;

    public TestHiveClientGlueMetastore()
    {
        super("test_glue" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
        // Glue requires a SerDe. Skip PAGEFILE because it doesn't have one.
        createTableFormats = difference(ImmutableSet.copyOf(createTableFormats), ImmutableSet.of(PAGEFILE));
    }

    @BeforeClass
    public void setUp()
    {
        executorService = newCachedThreadPool(daemonThreadsNamed("hive-glue-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    /**
     * GlueHiveMetastore currently uses AWS Default Credential Provider Chain,
     * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
     * on ways to set your AWS credentials which will be needed to run this test.
     */
    @Override
    protected ExtendedHiveMetastore createMetastore(File tempDir)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig();
        glueConfig.setDefaultWarehouseDir(tempDir.toURI().toString());
        glueConfig.setEnableGlueColumnStat(true);

        return new GlueHiveMetastore(hdfsEnvironment, glueConfig, executor);
    }

    @Override
    public void testRenameTable()
    {
        // rename table is not yet supported by Glue
    }

    @Override
    @Test
    public void testPartitionStatisticsSampling()
            throws Exception
    {
        super.testPartitionStatisticsSampling();
    }

    @Override
    @Test
    public void testUpdateTableColumnStatistics()
            throws Exception
    {
        // The original implementation AbstractTestHiveClient#testUpdateTableColumnStatistics assumes each update call to metastore would override
        // the previous update call but this is not entirely true for Glue.
        // For example, there are 5 columns of a table. if you update 2 columns and then update the remaining 3 columns in two separate calls.
        // The second update call would not override the first update call (assume the first 2 and remaining 3 columns are exclusive).
        // Following this rationale, provide an empty column stat does not clear all the existing column stats.
        // (Glue actually does not accept the empty column stat).
        SchemaTableName tableName = temporaryTable("update_table_column_statistics");
        try {
            doCreateEmptyTable(tableName, ORC, STATISTICS_TABLE_COLUMNS);
            // STATISTICS_1_1 must be subset of STATISTICS_1
            testUpdateTableStatistics(tableName, EMPTY_TABLE_STATISTICS, STATISTICS_1_1, STATISTICS_1);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Override
    @Test
    public void testUpdateTableColumnStatisticsEmptyOptionalFields()
    {
        // this test is not really meaningful for Glue, for numeric columns, min/max stat should always be available
        // The only exception is when all rows are NULLs. Glue requires min/max to be set.
    }

    @Override
    @Test
    public void testUpdatePartitionColumnStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_partition_column_statistics");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            testUpdatePartitionStatistics(
                    tableName,
                    EMPTY_TABLE_STATISTICS,
                    ImmutableList.of(STATISTICS_1_1, STATISTICS_1),
                    ImmutableList.of(STATISTICS_1_2, STATISTICS_1));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Override
    @Test
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
    {
        // this test is not really meaningful for Glue, for numeric columns, min/max stat should always be available
        // The only exception is when all rows are NULLs. Glue requires min/max to be set.
    }

    @Override
    @Test
    public void testStorePartitionWithStatistics()
            throws Exception
    {
        testStorePartitionWithStatistics(STATISTICS_PARTITIONED_TABLE_COLUMNS, BASIC_STATISTICS_1, BASIC_STATISTICS_2, BASIC_STATISTICS_1, EMPTY_TABLE_STATISTICS);
    }

    @Override
    @Test
    public void testGetPartitions() throws Exception
    {
        try {
            createDummyPartitionedTable(tablePartitionFormat, CREATE_TABLE_COLUMNS_PARTITIONED);
            Optional<List<String>> partitionNames = getMetastoreClient().getPartitionNames(tablePartitionFormat.getSchemaName(), tablePartitionFormat.getTableName());
            assertTrue(partitionNames.isPresent());
            assertEquals(partitionNames.get(), ImmutableList.of("ds=2016-01-01", "ds=2016-01-02"));
        }
        finally {
            dropTable(tablePartitionFormat);
        }
    }

    @Override
    protected void testUpdateTableStatistics(SchemaTableName tableName, PartitionStatistics initialStatistics, PartitionStatistics... statistics)
    {
        // The original implementation AbstractTestHiveClient#testUpdateTableStatistics assumes each update call to metastore would override
        // the previous update call but this is not entirely true for Glue.
        // For example, there are 5 columns of a table. if you update 2 columns and then update the remaining 3 columns in two separate calls.
        // The second update call would not override the first update call (assume the first 2 and remaining 3 columns are exclusive).
        // Following this rationale, provide an empty column stat does not clear all the existing column stats.
        // (Glue actually does not accept the empty column stat).
        ExtendedHiveMetastore metastoreClient = getMetastoreClient();
        assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                .isEqualTo(initialStatistics);

        AtomicReference<PartitionStatistics> expectedStatistics = new AtomicReference<>(initialStatistics);
        for (PartitionStatistics partitionStatistics : statistics) {
            metastoreClient.updateTableStatistics(tableName.getSchemaName(), tableName.getTableName(), actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatistics.get());
                return partitionStatistics;
            });
            assertThat(metastoreClient.getTableStatistics(tableName.getSchemaName(), tableName.getTableName()))
                    .isEqualTo(partitionStatistics);
            expectedStatistics.set(partitionStatistics);
        }
    }

    @Override
    protected void testUpdatePartitionStatistics(
            SchemaTableName tableName,
            PartitionStatistics initialStatistics,
            List<PartitionStatistics> firstPartitionStatistics,
            List<PartitionStatistics> secondPartitionStatistics)
    {
        // Same reason as testUpdateTableStatistics
        String firstPartitionName = "ds=2016-01-01";
        String secondPartitionName = "ds=2016-01-02";

        ExtendedHiveMetastore metastoreClient = getMetastoreClient();
        assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                .isEqualTo(ImmutableMap.of(firstPartitionName, initialStatistics, secondPartitionName, initialStatistics));

        AtomicReference<PartitionStatistics> expectedStatisticsPartition1 = new AtomicReference<>(initialStatistics);
        AtomicReference<PartitionStatistics> expectedStatisticsPartition2 = new AtomicReference<>(initialStatistics);

        for (int i = 0; i < firstPartitionStatistics.size(); i++) {
            PartitionStatistics statisticsPartition1 = firstPartitionStatistics.get(i);
            PartitionStatistics statisticsPartition2 = secondPartitionStatistics.get(i);
            metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), firstPartitionName, actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatisticsPartition1.get());
                return statisticsPartition1;
            });
            metastoreClient.updatePartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), secondPartitionName, actualStatistics -> {
                assertThat(actualStatistics).isEqualTo(expectedStatisticsPartition2.get());
                return statisticsPartition2;
            });
            assertThat(metastoreClient.getPartitionStatistics(tableName.getSchemaName(), tableName.getTableName(), ImmutableSet.of(firstPartitionName, secondPartitionName)))
                    .isEqualTo(ImmutableMap.of(firstPartitionName, statisticsPartition1, secondPartitionName, statisticsPartition2));
            expectedStatisticsPartition1.set(statisticsPartition1);
            expectedStatisticsPartition2.set(statisticsPartition2);
        }
    }
}
