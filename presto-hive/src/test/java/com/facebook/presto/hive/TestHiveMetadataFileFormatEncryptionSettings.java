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

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.InMemoryHiveMetastore;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
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
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.HIVE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.PARTITION_UPDATE_CODEC;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.google.common.collect.ImmutableMap.builder;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestHiveMetadataFileFormatEncryptionSettings
{
    private static final String TEST_SERVER_VERSION = "test_version";
    private static final String TEST_DB_NAME = "test_db";

    private HiveMetadataFactory metadataFactory;
    private HiveTransactionManager transactionManager;
    private ExtendedHiveMetastore metastore;
    private ExecutorService executor;
    private File baseDirectory;

    @BeforeClass
    public void setup()
    {
        baseDirectory = new File(Files.createTempDir(), "metastore");
        metastore = new BridgingHiveMetastore(new InMemoryHiveMetastore(baseDirectory));
        executor = newCachedThreadPool(daemonThreadsNamed("hive-encryption-test-%s"));
        transactionManager = new HiveTransactionManager();
        metadataFactory = new HiveMetadataFactory(
                metastore,
                HDFS_ENVIRONMENT,
                new HivePartitionManager(TYPE_MANAGER, HIVE_CLIENT_CONFIG),
                DateTimeZone.forTimeZone(TimeZone.getTimeZone(HIVE_CLIENT_CONFIG.getTimeZone())),
                true,
                false,
                false,
                false,
                true,
                HIVE_CLIENT_CONFIG.getMaxPartitionBatchSize(),
                HIVE_CLIENT_CONFIG.getMaxPartitionsPerScan(),
                TYPE_MANAGER,
                new HiveLocationService(HDFS_ENVIRONMENT),
                FUNCTION_RESOLUTION,
                ROW_EXPRESSION_SERVICE,
                FILTER_STATS_CALCULATOR_SERVICE,
                new TableParameterCodec(),
                PARTITION_UPDATE_CODEC,
                listeningDecorator(executor),
                new HiveTypeTranslator(),
                new HiveStagingFileCommitter(HDFS_ENVIRONMENT, listeningDecorator(executor)),
                new HiveZeroRowFileCreator(HDFS_ENVIRONMENT, new OutputStreamDataSinkFactory(), listeningDecorator(executor)),
                TEST_SERVER_VERSION,
                new HivePartitionObjectBuilder());

        metastore.createDatabase(Database.builder()
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

    private static ConnectorTableMetadata getConnectorTableMetadata(String tableName, Map<String, Object> tableProperties)
    {
        ImmutableMap.Builder<String, Object> properties = builder();
        properties.put(BUCKET_COUNT_PROPERTY, 0);
        properties.put(BUCKETED_BY_PROPERTY, ImmutableList.of());
        properties.put(SORTED_BY_PROPERTY, ImmutableList.of());
        properties.put(PARTITIONED_BY_PROPERTY, ImmutableList.of("ds"));
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
        ConnectorTableMetadata table = getConnectorTableMetadata(tableName, ImmutableMap.of(
                ENCRYPT_TABLE, "keyReference1",
                DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                DWRF_ENCRYPTION_PROVIDER, "test_provider"));

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
        ConnectorTableMetadata table = getConnectorTableMetadata(tableName, ImmutableMap.of(
                ENCRYPT_COLUMNS, ColumnEncryptionInformation.fromTableProperty(ImmutableList.of("key1:t_varchar,t_bigint", "key2: t_struct.char,t_struct.str.a")),
                DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                DWRF_ENCRYPTION_PROVIDER, "test_provider"));

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

    @Test
    public void testCreateTableAsSelectWithColumnKeyReference()
    {
        String tableName = "test_ctas_enc_with_column_key";
        ConnectorTableMetadata table = getConnectorTableMetadata(tableName, ImmutableMap.of(
                ENCRYPT_COLUMNS, ColumnEncryptionInformation.fromTableProperty(ImmutableList.of("key1:t_varchar,t_bigint", "key2: t_struct.char,t_struct.str.a")),
                DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                DWRF_ENCRYPTION_PROVIDER, "test_provider"));

        try {
            HiveMetadata metadata = metadataFactory.get();

            HiveOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, table, Optional.empty());
            metadata.finishCreateTable(SESSION, outputHandle, ImmutableList.of(), ImmutableList.of());

            ConnectorTableMetadata receivedMetadata = metadata.getTableMetadata(SESSION, new HiveTableHandle(TEST_DB_NAME, tableName));

            assertEquals(receivedMetadata.getProperties().get(ENCRYPT_COLUMNS), table.getProperties().get(ENCRYPT_COLUMNS));
            assertEquals(receivedMetadata.getProperties().get(DWRF_ENCRYPTION_ALGORITHM), table.getProperties().get(DWRF_ENCRYPTION_ALGORITHM));
            assertEquals(receivedMetadata.getProperties().get(DWRF_ENCRYPTION_PROVIDER), table.getProperties().get(DWRF_ENCRYPTION_PROVIDER));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Only one of encrypt_table or encrypt_columns should be specified")
    public void testTableCreationFailureWithColumnAndTableKeyReference()
    {
        String tableName = "test_enc_with_table_key_and_column_key";
        ConnectorTableMetadata table = getConnectorTableMetadata(tableName, ImmutableMap.of(
                ENCRYPT_TABLE, "tableKey",
                ENCRYPT_COLUMNS, ColumnEncryptionInformation.fromTableProperty(ImmutableList.of("key1:t_varchar,t_bigint", "key2: t_struct.char,t_struct.str.a")),
                DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                DWRF_ENCRYPTION_PROVIDER, "test_provider"));

        try {
            metadataFactory.get().createTable(SESSION, table, false);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Partition column \\(ds\\) cannot be used as an encryption column")
    public void testTableCreationFailureWithPartitionAsEncrypted()
    {
        String tableName = "test_enc_with_table_key_with_partition_enc";
        ConnectorTableMetadata table = getConnectorTableMetadata(tableName, ImmutableMap.of(
                ENCRYPT_COLUMNS, ColumnEncryptionInformation.fromTableProperty(ImmutableList.of("key1:ds")),
                DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                DWRF_ENCRYPTION_PROVIDER, "test_provider"));

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
        ConnectorTableMetadata table = getConnectorTableMetadata(tableName, ImmutableMap.of(
                ENCRYPT_COLUMNS, ColumnEncryptionInformation.fromTableProperty(ImmutableList.of("key1:this_column_does_not_exist")),
                DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                DWRF_ENCRYPTION_PROVIDER, "test_provider"));

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
        ConnectorTableMetadata table = getConnectorTableMetadata(tableName, ImmutableMap.of(
                ENCRYPT_COLUMNS, ColumnEncryptionInformation.fromTableProperty(ImmutableList.of("key1:t_struct.does_not_exist_subfield")),
                DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                DWRF_ENCRYPTION_PROVIDER, "test_provider"));

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
        ConnectorTableMetadata table = getConnectorTableMetadata(tableName, ImmutableMap.of(
                ENCRYPT_COLUMNS, ColumnEncryptionInformation.fromTableProperty(ImmutableList.of("key1:t_struct.str", "key2:t_struct.str.a")),
                DWRF_ENCRYPTION_ALGORITHM, "test_algo",
                DWRF_ENCRYPTION_PROVIDER, "test_provider"));

        try {
            metadataFactory.get().createTable(SESSION, table, false);
        }
        finally {
            dropTable(tableName);
        }
    }
}
