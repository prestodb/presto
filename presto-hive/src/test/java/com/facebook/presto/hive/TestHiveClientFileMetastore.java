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

import com.facebook.presto.execution.warnings.DefaultWarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.execution.warnings.WarningHandlingLevel;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveSplitManager.OBJECT_NOT_READABLE;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_QUERY_ID_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingContext;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveClientFileMetastore
        extends AbstractTestHiveClientLocal
{
    private static final SplitSchedulingContext SPLIT_SCHEDULING_CONTEXT = new SplitSchedulingContext(
            UNGROUPED_SCHEDULING,
            false,
            new DefaultWarningCollector(new WarningCollectorConfig(), WarningHandlingLevel.NORMAL));

    @Override
    protected ExtendedHiveMetastore createMetastore(File tempDir)
    {
        File baseDir = new File(tempDir, "metastore");
        HiveClientConfig hiveConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfigurationInitializer updator = new HdfsConfigurationInitializer(hiveConfig, metastoreClientConfig);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updator, ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
    }

    @Override
    public void testMismatchSchemaTable()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testPartitionSchemaMismatch()
    {
        // test expects an exception to be thrown
        throw new SkipException("FileHiveMetastore only supports replaceTable() for views");
    }

    @Override
    public void testBucketedTableEvolution()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testBucketedTableEvolutionWithDifferentReadBucketCount()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testTransactionDeleteInsert()
    {
        // FileHiveMetastore has various incompatibilities
    }

    @Test
    public void testPartitionNotReadable()
    {
        SchemaTableName tableName = temporaryTable("tempTable");
        Map<String, String> dynamicPartitionParameters = ImmutableMap.of(OBJECT_NOT_READABLE, "Testing Unreadable Partition");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS, dynamicPartitionParameters);

            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                ConnectorSession session = newSession();

                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                assertNotNull(tableHandle);

                ColumnHandle dsColumn = metadata.getColumnHandles(session, tableHandle).get("ds");
                assertNotNull(dsColumn);

                ConnectorTableLayout tableLayout = getTableLayout(session, metadata, tableHandle, Constraint.alwaysTrue(), transaction);
                try {
                    getSplitCount(splitManager.getSplits(transaction.getTransactionHandle(), session, tableLayout.getHandle(), SPLIT_SCHEDULING_CONTEXT));
                    fail("Expected HiveNotReadableException");
                }
                catch (HiveNotReadableException e) {
                    assertEquals(e.getTableName(), tableName);
                    assertNotNull(SPLIT_SCHEDULING_CONTEXT.getWarningCollector());
                    assertEquals(SPLIT_SCHEDULING_CONTEXT.getWarningCollector().getWarnings().size(), 0);
                }
            }

            try (Transaction transaction = newTransaction()) {
                ConnectorMetadata metadata = transaction.getMetadata();
                ConnectorSession session = newSession(ImmutableMap.of(HiveSessionProperties.IGNORE_UNREADABLE_PARTITION, true));

                ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
                assertNotNull(tableHandle);

                ColumnHandle dsColumn = metadata.getColumnHandles(session, tableHandle).get("ds");
                assertNotNull(dsColumn);

                ConnectorTableLayout tableLayout = getTableLayout(session, metadata, tableHandle, Constraint.alwaysTrue(), transaction);
                splitManager.getSplits(transaction.getTransactionHandle(), session, tableLayout.getHandle(), SPLIT_SCHEDULING_CONTEXT);
                assertNotNull(SPLIT_SCHEDULING_CONTEXT.getWarningCollector());
                assertEquals(SPLIT_SCHEDULING_CONTEXT.getWarningCollector().getWarnings().size(), 1);
                assertTrue(SPLIT_SCHEDULING_CONTEXT.getWarningCollector().getWarnings().get(0).getMessage().contains("has 1 out of 3 partitions unreadable: ds=2020-01-03... are due to Testing Unreadable Partition. "));
                assertEquals(SPLIT_SCHEDULING_CONTEXT.getWarningCollector().getWarnings().get(0).getWarningCode().getName(), "PARTITION_NOT_READABLE");
            }
        }
        catch (Exception e) {
            fail("Exception not expected");
        }
        finally {
            dropTable(tableName);
        }
    }

    private void createDummyPartitionedTable(SchemaTableName tableName, List<ColumnMetadata> columns, Map<String, String> dynamicPartitionParameters)
            throws Exception
    {
        doCreateEmptyTable(tableName, ORC, columns);

        ExtendedHiveMetastore metastoreClient = getMetastoreClient();
        Table table = metastoreClient.getTable(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        List<String> firstPartitionValues = ImmutableList.of("2020-01-01");
        List<String> secondPartitionValues = ImmutableList.of("2020-01-02");
        List<String> thirdPartitionValues = ImmutableList.of("2020-01-03");

        String firstPartitionName = makePartName(ImmutableList.of("ds"), firstPartitionValues);
        String secondPartitionName = makePartName(ImmutableList.of("ds"), secondPartitionValues);
        String thirdPartitionName = makePartName(ImmutableList.of("ds"), thirdPartitionValues);

        List<PartitionWithStatistics> partitions = ImmutableList.of(firstPartitionName, secondPartitionName)
                .stream()
                .map(partitionName -> new PartitionWithStatistics(createDummyPartition(table, partitionName), partitionName, PartitionStatistics.empty()))
                .collect(toImmutableList());
        metastoreClient.addPartitions(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), partitions);
        metastoreClient.updatePartitionStatistics(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), firstPartitionName, currentStatistics -> EMPTY_TABLE_STATISTICS);
        metastoreClient.updatePartitionStatistics(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), secondPartitionName, currentStatistics -> EMPTY_TABLE_STATISTICS);

        List<PartitionWithStatistics> partitionsNotReadable = ImmutableList.of(thirdPartitionName)
                .stream()
                .map(partitionName -> new PartitionWithStatistics(createDummyPartition(table, partitionName, dynamicPartitionParameters), partitionName, PartitionStatistics.empty()))
                .collect(toImmutableList());
        metastoreClient.addPartitions(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), partitionsNotReadable);
        metastoreClient.updatePartitionStatistics(METASTORE_CONTEXT, tableName.getSchemaName(), tableName.getTableName(), thirdPartitionName, currentStatistics -> EMPTY_TABLE_STATISTICS);
    }

    private Partition createDummyPartition(Table table, String partitionName, Map<String, String> dynamicPartitionParameters)
    {
        return createDummyPartition(table, partitionName, Optional.empty(), dynamicPartitionParameters);
    }

    private Partition createDummyPartition(Table table, String partitionName, Optional<HiveBucketProperty> bucketProperty, Map<String, String> dynamicPartitionParameters)
    {
        Map<String, String> staticPartitionParameters = ImmutableMap.of(
                PRESTO_VERSION_NAME, "testversion",
                PRESTO_QUERY_ID_NAME, "20200101_123456_00001_x1y2z");
        Map<String, String> partitionParameters = ImmutableMap.<String, String>builder()
                .putAll(staticPartitionParameters)
                .putAll(dynamicPartitionParameters)
                .build();
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(toPartitionValues(partitionName))
                .withStorage(storage -> storage
                        .setStorageFormat(fromHiveStorageFormat(HiveStorageFormat.ORC))
                        .setLocation(partitionTargetPath(new SchemaTableName(table.getDatabaseName(), table.getTableName()), partitionName))
                        .setBucketProperty(bucketProperty))
                .setParameters(partitionParameters)
                .setEligibleToIgnore(true)
                .setSealedPartition(true)
                .build();
    }
}
