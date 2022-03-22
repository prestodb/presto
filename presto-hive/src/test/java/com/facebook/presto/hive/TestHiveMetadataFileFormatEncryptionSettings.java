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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.InMemoryHiveMetastore;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.ColumnEncryptionInformation.fromTableProperty;
import static com.facebook.presto.hive.EncryptionInformation.fromEncryptionMetadata;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveSessionProperties.NEW_PARTITION_USER_SUPPLIED_PARAMETER;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.DWRF_ENCRYPTION_ALGORITHM;
import static com.facebook.presto.hive.HiveTableProperties.DWRF_ENCRYPTION_PROVIDER;
import static com.facebook.presto.hive.HiveTableProperties.ENCRYPT_COLUMNS;
import static com.facebook.presto.hive.HiveTableProperties.ENCRYPT_TABLE;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTestUtils.FILTER_STATS_CALCULATOR_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.HIVE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.PARTITION_UPDATE_CODEC;
import static com.facebook.presto.hive.HiveTestUtils.PARTITION_UPDATE_SMILE_CODEC;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.APPEND;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.NEW;
import static com.facebook.presto.hive.PartitionUpdate.UpdateMode.OVERWRITE;
import static com.facebook.presto.hive.TestDwrfEncryptionInformationSource.TEST_EXTRA_METADATA;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveMetadataFileFormatEncryptionSettings
{
    private static final String TEST_SERVER_VERSION = "test_version";
    private static final String TEST_DB_NAME = "test_db";
    private static final JsonCodec<PartitionUpdate> PARTITION_CODEC = jsonCodec(PartitionUpdate.class);
    private static final ConnectorSession SESSION = new TestingConnectorSession(
            new HiveSessionProperties(new HiveClientConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig(), new CacheConfig()).getSessionProperties(),
            ImmutableMap.of(NEW_PARTITION_USER_SUPPLIED_PARAMETER, "{}"));

    private HiveMetadataFactory metadataFactory;
    private HiveTransactionManager transactionManager;
    private ExtendedHiveMetastore metastore;
    private ExecutorService executor;
    private File baseDirectory;

    @BeforeClass
    public void setup()
    {
        baseDirectory = new File(Files.createTempDir(), "metastore");
        metastore = new BridgingHiveMetastore(new InMemoryHiveMetastore(baseDirectory), new HivePartitionMutator());
        executor = newCachedThreadPool(daemonThreadsNamed("hive-encryption-test-%s"));
        transactionManager = new HiveTransactionManager();
        metadataFactory = new HiveMetadataFactory(
                metastore,
                HDFS_ENVIRONMENT,
                new HivePartitionManager(FUNCTION_AND_TYPE_MANAGER, HIVE_CLIENT_CONFIG),
                DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of(HIVE_CLIENT_CONFIG.getTimeZone()))),
                true,
                false,
                false,
                false,
                true,
                true,
                HIVE_CLIENT_CONFIG.getMaxPartitionBatchSize(),
                HIVE_CLIENT_CONFIG.getMaxPartitionsPerScan(),
                false,
                FUNCTION_AND_TYPE_MANAGER,
                new HiveLocationService(HDFS_ENVIRONMENT),
                FUNCTION_RESOLUTION,
                ROW_EXPRESSION_SERVICE,
                FILTER_STATS_CALCULATOR_SERVICE,
                new TableParameterCodec(),
                PARTITION_UPDATE_CODEC,
                PARTITION_UPDATE_SMILE_CODEC,
                listeningDecorator(executor),
                new HiveTypeTranslator(),
                new HiveStagingFileCommitter(HDFS_ENVIRONMENT, listeningDecorator(executor)),
                new HiveZeroRowFileCreator(HDFS_ENVIRONMENT, new OutputStreamDataSinkFactory(), listeningDecorator(executor)),
                TEST_SERVER_VERSION,
                new HivePartitionObjectBuilder(),
                new HiveEncryptionInformationProvider(ImmutableList.of(new TestDwrfEncryptionInformationSource())),
                new HivePartitionStats(),
                new HiveFileRenamer());

        metastore.createDatabase(METASTORE_CONTEXT, Database.builder()
                .setDatabaseName(TEST_DB_NAME)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build());
    }

    @AfterClass
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    private static ConnectorTableMetadata getConnectorTableMetadata(String tableName, Map<String, Object> tableProperties, boolean isPartitioned)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(BUCKET_COUNT_PROPERTY, 0);
        properties.put(BUCKETED_BY_PROPERTY, ImmutableList.of());
        properties.put(SORTED_BY_PROPERTY, ImmutableList.of());
        if (isPartitioned) {
            properties.put(PARTITIONED_BY_PROPERTY, ImmutableList.of("ds"));
        }
        properties.put(STORAGE_FORMAT_PROPERTY, DWRF);

        properties.putAll(tableProperties);
        return new ConnectorTableMetadata(
                new SchemaTableName(TEST_DB_NAME, tableName),
                ImmutableList.of(
                        new ColumnMetadata("t_varchar", VARCHAR),
                        new ColumnMetadata("t_bigint", BIGINT),
                        new ColumnMetadata("t_struct", RowType.from(
                                ImmutableList.of(
                                        new RowType.Field(Optional.of("char"), VARCHAR),
                                        new RowType.Field(Optional.of("str"), RowType.from(
                                                ImmutableList.of(
                                                        new RowType.Field(Optional.of("a"), VARCHAR),
                                                        new RowType.Field(Optional.of("b"), BIGINT))))))),
                        new ColumnMetadata("ds", VARCHAR)),
                properties.build());
    }

    private void dropTable(String tableName)
    {
        try {
            metadataFactory.get().dropTable(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));
        }
        catch (TableNotFoundException ex) {
            // Do nothing
        }
    }

    @Test
    public void testTableCreationWithTableKeyReference()
    {
        String tableName = "test_enc_with_table_key";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_TABLE, "keyReference1",
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            HiveMetadata metadata = metadataFactory.get();
            metadata.createTable(SESSION, table, false);
            ConnectorTableMetadata receivedMetadata = metadata.getTableMetadata(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));

            assertEquals(receivedMetadata.getProperties().get(ENCRYPT_TABLE), table.getProperties().get(ENCRYPT_TABLE));
            assertEquals(receivedMetadata.getProperties().get(DWRF_ENCRYPTION_ALGORITHM), table.getProperties().get(DWRF_ENCRYPTION_ALGORITHM));
            assertEquals(receivedMetadata.getProperties().get(DWRF_ENCRYPTION_PROVIDER), table.getProperties().get(DWRF_ENCRYPTION_PROVIDER));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testTableCreationWithColumnKeyReference()
    {
        String tableName = "test_enc_with_column_key";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:t_varchar,t_bigint", "key2: t_struct.char,t_struct.str.a")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            HiveMetadata metadata = metadataFactory.get();
            metadata.createTable(SESSION, table, false);
            ConnectorTableMetadata receivedMetadata = metadata.getTableMetadata(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));

            assertEquals(receivedMetadata.getProperties().get(ENCRYPT_COLUMNS), table.getProperties().get(ENCRYPT_COLUMNS));
            assertEquals(receivedMetadata.getProperties().get(DWRF_ENCRYPTION_ALGORITHM), table.getProperties().get(DWRF_ENCRYPTION_ALGORITHM));
            assertEquals(receivedMetadata.getProperties().get(DWRF_ENCRYPTION_PROVIDER), table.getProperties().get(DWRF_ENCRYPTION_PROVIDER));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(description = "This also tests that NEW_PARTITION_USER_SUPPLIED_PARAMETER also works")
    public void testCreateTableAsSelectWithColumnKeyReference()
    {
        String tableName = "test_ctas_enc_with_column_key";
        ColumnEncryptionInformation columnEncryptionInformation = fromTableProperty(ImmutableList.of("key1:t_varchar,t_bigint", "key2: t_struct.char,t_struct.str.a"));
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, columnEncryptionInformation,
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            HiveMetadata metadata = metadataFactory.get();

            HiveOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, table, Optional.empty());

            assertTrue(outputHandle.getEncryptionInformation().isPresent());
            assertEquals(
                    outputHandle.getEncryptionInformation().get(),
                    fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(
                            columnEncryptionInformation.getColumnToKeyReference().entrySet().stream()
                                    .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().getBytes())),
                            ImmutableMap.of(TEST_EXTRA_METADATA, "test_algo"),
                            "test_algo",
                            "test_provider")));

            List<PartitionUpdate> partitionUpdates = ImmutableList.of(
                    new PartitionUpdate("ds=2020-06-26", NEW, "path1", "path1", ImmutableList.of(), 0, 0, 0, false),
                    new PartitionUpdate("ds=2020-06-27", NEW, "path2", "path2", ImmutableList.of(), 0, 0, 0, false));

            metadata.finishCreateTable(
                    SESSION,
                    outputHandle,
                    partitionUpdates.stream().map(update -> Slices.utf8Slice(PARTITION_CODEC.toJson(update))).collect(toImmutableList()),
                    ImmutableList.of());

            ConnectorTableMetadata receivedMetadata = metadata.getTableMetadata(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));

            assertEquals(receivedMetadata.getProperties().get(ENCRYPT_COLUMNS), table.getProperties().get(ENCRYPT_COLUMNS));
            assertEquals(receivedMetadata.getProperties().get(DWRF_ENCRYPTION_ALGORITHM), table.getProperties().get(DWRF_ENCRYPTION_ALGORITHM));
            assertEquals(receivedMetadata.getProperties().get(DWRF_ENCRYPTION_PROVIDER), table.getProperties().get(DWRF_ENCRYPTION_PROVIDER));

            metadata.commit();

            Map<String, Optional<Partition>> partitions = metastore.getPartitionsByNames(METASTORE_CONTEXT, TEST_DB_NAME, tableName, ImmutableList.of("ds=2020-06-26", "ds=2020-06-27"));
            assertEquals(partitions.get("ds=2020-06-26").get().getParameters().get(TEST_EXTRA_METADATA), "test_algo");
            assertEquals(partitions.get("ds=2020-06-27").get().getParameters().get(TEST_EXTRA_METADATA), "test_algo");
            // Checking NEW_PARTITION_USER_SUPPLIED_PARAMETER
            assertEquals(partitions.get("ds=2020-06-27").get().getParameters().get("user_supplied"), "{}");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Only one of encrypt_table or encrypt_columns should be specified")
    public void testTableCreationFailureWithColumnAndTableKeyReference()
    {
        String tableName = "test_enc_with_table_key_and_column_key";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_TABLE, "tableKey",
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:t_varchar,t_bigint", "key2: t_struct.char,t_struct.str.a")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            metadataFactory.get().createTable(SESSION, table, false);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Creating an encrypted table without partitions is not supported.*")
    public void testTableCreationFailureWithEncryptionAndUnpartitionedTable()
    {
        String tableName = "test_enc_with_unpartitioned_table";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_TABLE, "tableKey",
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                false);

        try {
            metadataFactory.get().createTable(SESSION, table, false);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Partition column \\(ds\\) cannot be used as an encryption column")
    public void testTableCreationFailureWithPartitionColumnEncrypted()
    {
        String tableName = "test_enc_with_table_key_with_partition_enc";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:ds")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            metadataFactory.get().createTable(SESSION, table, false);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "In encrypt_columns unable to find column this_column_does_not_exist")
    public void testTableCreationFailureWithUnknownColumnAsEncrypted()
    {
        String tableName = "test_enc_with_table_key_with_unknown_col_enc";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:this_column_does_not_exist")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            metadataFactory.get().createTable(SESSION, table, false);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "In encrypt_columns subfields declared in t_struct.does_not_exist_subfield, but t_struct has type .*")
    public void testTableCreationFailureWithUnknownSubfieldAsEncrypted()
    {
        String tableName = "test_enc_with_table_key_with_unknown_col_enc";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:t_struct.does_not_exist_subfield")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            metadataFactory.get().createTable(SESSION, table, false);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "For \\(t_struct.str.a\\) found a keyReference at a higher level field \\(t_struct.str\\)")
    public void testTableCreationFailureWithSubfieldAlsoHavingASetting()
    {
        String tableName = "test_enc_with_table_key_with_subfield_also_having_a_setting";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:t_struct.str", "key2:t_struct.str.a")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            metadataFactory.get().createTable(SESSION, table, false);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertIntoPartitionedTable()
    {
        String tableName = "test_enc_with_insert_partitioned_table";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:t_struct.str")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            HiveMetadata createHiveMetadata = metadataFactory.get();
            createHiveMetadata.createTable(SESSION, table, false);
            HiveInsertTableHandle insertTableHandle = createHiveMetadata.beginInsert(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));

            assertTrue(insertTableHandle.getEncryptionInformation().isPresent());
            assertEquals(
                    insertTableHandle.getEncryptionInformation().get(),
                    fromEncryptionMetadata(
                            DwrfEncryptionMetadata.forPerField(
                                    ImmutableMap.of("t_struct.str", "key1".getBytes()),
                                    ImmutableMap.of(TEST_EXTRA_METADATA, "test_algo"),
                                    "test_algo",
                                    "test_provider")));

            List<PartitionUpdate> partitionUpdates = ImmutableList.of(
                    new PartitionUpdate("ds=2020-06-26", NEW, "path1", "path1", ImmutableList.of(), 0, 0, 0, false),
                    new PartitionUpdate("ds=2020-06-27", NEW, "path2", "path2", ImmutableList.of(), 0, 0, 0, false));

            createHiveMetadata.finishInsert(
                    SESSION,
                    insertTableHandle,
                    partitionUpdates.stream().map(update -> Slices.utf8Slice(PARTITION_CODEC.toJson(update))).collect(toImmutableList()),
                    ImmutableList.of());
            createHiveMetadata.commit();

            Map<String, Optional<Partition>> partitions = metastore.getPartitionsByNames(METASTORE_CONTEXT, TEST_DB_NAME, tableName, ImmutableList.of("ds=2020-06-26", "ds=2020-06-27"));
            assertEquals(partitions.get("ds=2020-06-26").get().getStorage().getLocation(), "path1");
            assertEquals(partitions.get("ds=2020-06-26").get().getParameters().get(TEST_EXTRA_METADATA), "test_algo");
            assertEquals(partitions.get("ds=2020-06-27").get().getParameters().get(TEST_EXTRA_METADATA), "test_algo");

            HiveMetadata overrideHiveMetadata = metadataFactory.get();
            insertTableHandle = overrideHiveMetadata.beginInsert(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));
            partitionUpdates = ImmutableList.of(
                    new PartitionUpdate("ds=2020-06-26", OVERWRITE, "path3", "path3", ImmutableList.of(), 0, 0, 0, false));

            overrideHiveMetadata.finishInsert(
                    SESSION,
                    insertTableHandle,
                    partitionUpdates.stream().map(update -> Slices.utf8Slice(PARTITION_CODEC.toJson(update))).collect(toImmutableList()),
                    ImmutableList.of());
            overrideHiveMetadata.commit();

            partitions = metastore.getPartitionsByNames(METASTORE_CONTEXT, TEST_DB_NAME, tableName, ImmutableList.of("ds=2020-06-26"));
            assertEquals(partitions.get("ds=2020-06-26").get().getStorage().getLocation(), "path3");
            assertEquals(partitions.get("ds=2020-06-26").get().getParameters().get(TEST_EXTRA_METADATA), "test_algo");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "For encrypted tables, partition format \\(ORC\\) should match table format \\(DWRF\\).*")
    public void testTableCreationWithInsertIntoNonDwrfPartition()
    {
        String tableName = "test_enc_with_create_partitioned_table_insert_non_dwrf_partition";

        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setRespectTableFormat(false).setHiveStorageFormat(ORC),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties());

        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:t_struct.str")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            HiveMetadata createHiveMetadata = metadataFactory.get();
            createHiveMetadata.beginCreateTable(session, table, Optional.empty());
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "For encrypted tables, partition format \\(ORC\\) should match table format \\(DWRF\\).*")
    public void testFailureWithInsertIntoPartitionedTableWithNonDwrfPartition()
    {
        String tableName = "test_enc_with_insert_partitioned_table_non_dwrf_partition";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:t_struct.str")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            HiveMetadata createHiveMetadata = metadataFactory.get();
            createHiveMetadata.createTable(SESSION, table, false);
            createHiveMetadata.commit();

            HiveMetadata insertHiveMetadata = metadataFactory.get();

            ConnectorSession newSession = new TestingConnectorSession(
                    new HiveSessionProperties(
                            new HiveClientConfig().setRespectTableFormat(false).setHiveStorageFormat(ORC),
                            new OrcFileWriterConfig(),
                            new ParquetFileWriterConfig(),
                            new CacheConfig()).getSessionProperties());

            insertHiveMetadata.beginInsert(newSession, new HiveTableHandle(TEST_DB_NAME, tableName));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Inserting into an existing partition with encryption enabled is not supported yet")
    public void testInsertIntoExistingPartition()
    {
        String tableName = "test_enc_with_insert_existing_partition";
        ConnectorTableMetadata table = getConnectorTableMetadata(
                tableName,
                ImmutableMap.of(
                        ENCRYPT_COLUMNS, fromTableProperty(ImmutableList.of("key1:t_struct.str")),
                        DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                        DWRF_ENCRYPTION_PROVIDER, "test_provider"),
                true);

        try {
            HiveMetadata createHiveMetadata = metadataFactory.get();
            createHiveMetadata.createTable(SESSION, table, false);
            HiveInsertTableHandle insertTableHandle = createHiveMetadata.beginInsert(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));

            List<PartitionUpdate> partitionUpdates = ImmutableList.of(
                    new PartitionUpdate("ds=2020-06-26", NEW, "path1", "path1", ImmutableList.of(), 0, 0, 0, false));

            createHiveMetadata.finishInsert(
                    SESSION,
                    insertTableHandle,
                    partitionUpdates.stream().map(update -> Slices.utf8Slice(PARTITION_CODEC.toJson(update))).collect(toImmutableList()),
                    ImmutableList.of());
            createHiveMetadata.commit();

            HiveMetadata appendHiveMetadata = metadataFactory.get();
            insertTableHandle = appendHiveMetadata.beginInsert(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));
            partitionUpdates = ImmutableList.of(
                    new PartitionUpdate("ds=2020-06-26", APPEND, "path3", "path3", ImmutableList.of(), 0, 0, 0, false));

            appendHiveMetadata.finishInsert(
                    SESSION,
                    insertTableHandle,
                    partitionUpdates.stream().map(update -> Slices.utf8Slice(PARTITION_CODEC.toJson(update))).collect(toImmutableList()),
                    ImmutableList.of());
        }
        finally {
            dropTable(tableName);
        }
    }
}
