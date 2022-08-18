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

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.TableInput;
import com.facebook.presto.hive.AbstractTestHiveClientLocal;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.DELTA_LAKE_PROVIDER;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_VALUE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.SPARK_TABLE_PROVIDER_KEY;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isDeltaLakeTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isIcebergTable;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveClientGlueMetastore
        extends AbstractTestHiveClientLocal
{
    private ExecutorService executorService;

    public TestHiveClientGlueMetastore()
    {
        super("test_glue" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
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
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of(), hiveClientConfig);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig();
        glueConfig.setDefaultWarehouseDir(tempDir.toURI().toString());

        return new GlueHiveMetastore(hdfsEnvironment, glueConfig, executor);
    }

    @Override
    public void testRenameTable()
    {
        // rename table is not yet supported by Glue
    }

    @Override
    public void testPartitionStatisticsSampling()
            throws Exception
    {
        // Glue metastore does not support column level statistics
    }

    @Override
    public void testUpdateTableColumnStatistics()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdateTableColumnStatisticsEmptyOptionalFields()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdatePartitionColumnStatistics()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testTableConstraints()
    {
        // GlueMetastore has no support for table constraints
    }

    @Override
    public void testStorePartitionWithStatistics()
            throws Exception
    {
        testStorePartitionWithStatistics(STATISTICS_PARTITIONED_TABLE_COLUMNS, BASIC_STATISTICS_1, BASIC_STATISTICS_2, BASIC_STATISTICS_1, EMPTY_TABLE_STATISTICS);
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        try {
            createDummyPartitionedTable(tablePartitionFormat, CREATE_TABLE_COLUMNS_PARTITIONED);
            Optional<List<String>> partitionNames = getMetastoreClient().getPartitionNames(METASTORE_CONTEXT, tablePartitionFormat.getSchemaName(), tablePartitionFormat.getTableName());
            assertTrue(partitionNames.isPresent());
            assertEquals(partitionNames.get(), ImmutableList.of("ds=2016-01-01", "ds=2016-01-02"));
        }
        finally {
            dropTable(tablePartitionFormat);
        }
    }

    @Test
    public void testTableWithoutStorageDescriptor()
    {
        // StorageDescriptor is an Optional field for Glue tables. Iceberg and Delta Lake tables may not have it set.
        SchemaTableName table = temporaryTable("test_missing_storage_descriptor");
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(table.getSchemaName())
                .withName(table.getTableName());
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        try {
            ConnectorSession session = newSession();
            MetastoreContext metastoreContext = new MetastoreContext(
                    session.getIdentity(),
                    session.getQueryId(),
                    session.getClientInfo(),
                    session.getSource(),
                    getMetastoreHeaders(session),
                    false,
                    DEFAULT_COLUMN_CONVERTER_PROVIDER);
            TableInput tableInput = new TableInput()
                    .withName(table.getTableName())
                    .withTableType(EXTERNAL_TABLE.name());
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));

            assertThatThrownBy(() -> getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()))
                    .hasMessageStartingWith("Table StorageDescriptor is null for table");
            glueClient.deleteTable(deleteTableRequest);

            // Iceberg table
            tableInput = tableInput.withParameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE));
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));
            assertTrue(isIcebergTable(getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()).orElseThrow(() -> new NoSuchElementException())));
            glueClient.deleteTable(deleteTableRequest);

            // Delta Lake table
            tableInput = tableInput.withParameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER));
            glueClient.createTable(new CreateTableRequest()
                    .withDatabaseName(database)
                    .withTableInput(tableInput));
            assertTrue(isDeltaLakeTable(getMetastoreClient().getTable(metastoreContext, table.getSchemaName(), table.getTableName()).orElseThrow(() -> new NoSuchElementException())));
        }
        finally {
            // Table cannot be dropped through HiveMetastore since a TableHandle cannot be created
            glueClient.deleteTable(new DeleteTableRequest()
                    .withDatabaseName(table.getSchemaName())
                    .withName(table.getTableName()));
        }
    }
}
