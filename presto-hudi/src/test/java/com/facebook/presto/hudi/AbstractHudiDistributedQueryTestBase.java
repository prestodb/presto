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

package com.facebook.presto.hudi;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.io.Resources;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.testng.annotations.AfterClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.facebook.presto.hive.HiveType.HIVE_BINARY;
import static com.facebook.presto.hive.HiveType.HIVE_BOOLEAN;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public abstract class AbstractHudiDistributedQueryTestBase
        extends AbstractTestQueryFramework
{
    public static final String HUDI_CATALOG = "hudi";

    public static final String HUDI_SCHEMA = "testing"; // Schema in Hive which has test hudi tables

    protected static ExtendedHiveMetastore metastore;

    private static final String OWNER_PUBLIC = "public";
    protected static final MetastoreContext METASTORE_CONTEXT = new MetastoreContext("test_user", "test_queryId", Optional.empty(), Optional.empty(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
    private static final PrincipalPrivileges PRINCIPAL_PRIVILEGES = new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of());

    private static final StorageFormat STORAGE_FORMAT_COPY_ON_WRITE = StorageFormat.create(
            ParquetHiveSerDe.class.getName(),
            HoodieParquetInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName());
    private static final StorageFormat STORAGE_FORMAT_MERGE_ON_READ = StorageFormat.create(
            ParquetHiveSerDe.class.getName(),
            HoodieParquetRealtimeInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName());

    //     spark.sql(
    //             """create table data_partition_prune
    //     |(id int, comb int, col0 int, col1 bigint, col2 float, col3 double,
    //     | col4 string, col5 date, col6 boolean, col7 binary, year int, month int, day int)
    //     | using hudi
    //     | partitioned by (year,month,day)
    //     | options(
    //     | type='cow', primaryKey='id', preCombineField='comb',
    //     | hoodie.metadata.index.column.stats.enable = "true",
    //     | hoodie.metadata.index.column.stats.file.group.count = "1",
    //     | hoodie.metadata.index.column.stats.column.list = 'col0,col3,col4,col5',
    //     | 'hoodie.metadata.enable'='true')""".stripMargin)
    //
    //     spark.sql(
    // s"""
    //      | insert into data_partition_prune values
    //      | (1,1,99,1111111,101.01,1001.0001,'x000001','2021-12-25',true,'a01',2022, 11, 12),
    //      | (2,2,99,1111111,102.02,1002.0002,'x000002','2021-12-25',true,'a02',2022, 10, 30),
    //      | (3,3,99,1111111,103.03,1003.0003,'x000003','2021-12-25',false,'a03',2021, 10, 11),
    //      | (4,4,99,1111111,104.04,1004.0004,'x000004','2021-12-26',true,'a04',2021, 11, 12)
    //      |""".stripMargin)
    public static final List<Column> DATA_COLUMNS = ImmutableList.of(
            column("id", HIVE_INT),
            column("comb", HIVE_INT),
            column("col0", HIVE_INT),
            column("col1", HIVE_LONG),
            column("col2", HIVE_FLOAT),
            column("col3", HIVE_DOUBLE),
            column("col4", HIVE_STRING),
            column("col5", HIVE_DATE),
            column("col6", HIVE_BOOLEAN),
            column("col7", HIVE_BINARY));

    public static final List<Column> PARTITION_COLUMNS = ImmutableList.of(column("year", HIVE_INT), column("month", HIVE_INT), column("day", HIVE_INT));
    public static final List<Column> HUDI_META_COLUMNS = ImmutableList.of(
            column("_hoodie_commit_time", HiveType.HIVE_STRING),
            column("_hoodie_commit_seqno", HiveType.HIVE_STRING),
            column("_hoodie_record_key", HiveType.HIVE_STRING),
            column("_hoodie_partition_path", HiveType.HIVE_STRING),
            column("_hoodie_file_name", HiveType.HIVE_STRING));

    /**
     * List of tables present in the test resources directory.
     * used to test dataskipping/partition prune.
     */
    protected static final String HUDI_SKIPPING_TABLE = "data_partition_prune";
    protected static final String HUDI_SKIPPING_TABLE_NON_HIVE_STYLE = "data_partition_prune_non_hive_style_partition";
    protected static ConnectorSession connectorSession;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHudiQueryRunner(ImmutableMap.of());
    }

    @AfterClass
    public void deleteTestHudiTables()
    {
        QueryRunner queryRunner = getQueryRunner();
        if (queryRunner != null) {
            // Remove the test hudi tables from HMS
            metastore.dropTable(METASTORE_CONTEXT, HUDI_SCHEMA, HUDI_SKIPPING_TABLE, false);
            metastore.dropTable(METASTORE_CONTEXT, HUDI_SCHEMA, HUDI_SKIPPING_TABLE_NON_HIVE_STYLE, false);
        }
    }

    protected static String getTablePath(String tableName, Path dataDir)
    {
        return "file://" + dataDir.resolve(tableName);
    }

    private static DistributedQueryRunner createHudiQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(HUDI_CATALOG)
                .setSchema(HUDI_SCHEMA)
                .setCatalogSessionProperty(HUDI_CATALOG, "hudi_metadata_table_enabled", "true")
                .setConnectionProperty(new ConnectorId("hudi"), "hudi_metadata_table_enabled", "true")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();

        // setup file metastore

        Path catalogDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("catalog");

        metastore = createFileHiveMetastore(catalogDirectory.toString());

        // create database
        Database database = Database.builder()
                .setDatabaseName(HUDI_SCHEMA)
                .setOwnerName(OWNER_PUBLIC)
                .setOwnerType(PrincipalType.ROLE)
                .build();
        metastore.createDatabase(METASTORE_CONTEXT, database);

        Path testingDataDir = queryRunner.getCoordinator().getDataDirectory().resolve("data");

        try (InputStream stream = Resources.getResource("hudi-skipping-schema-data.zip").openStream()) {
            unzip(stream, testingDataDir);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Create the test hudi tables for dataSkipping/partition prune in HMS
        registerHudiTableInHMS(HoodieTableType.COPY_ON_WRITE, HUDI_SKIPPING_TABLE, testingDataDir, Streams.concat(HUDI_META_COLUMNS.stream(), DATA_COLUMNS.stream()).collect(Collectors.toList()));

        // Create the test hudi tables with non_hive_style_partitions for dataSkipping/partition prune in HMS
        registerHudiTableInHMS(HoodieTableType.COPY_ON_WRITE, HUDI_SKIPPING_TABLE_NON_HIVE_STYLE, testingDataDir, Streams.concat(HUDI_META_COLUMNS.stream(), DATA_COLUMNS.stream()).collect(Collectors.toList()));

        // Install a hudi connector catalog
        queryRunner.installPlugin(new HudiPlugin("hudi", Optional.of(metastore)));
        Map<String, String> hudiProperties = ImmutableMap.<String, String>builder().build();
        queryRunner.createCatalog(HUDI_CATALOG, "hudi", hudiProperties);

        connectorSession = queryRunner.getDefaultSession().toConnectorSession(new ConnectorId(session.getCatalog().get()));

        return queryRunner;
    }

    private static ExtendedHiveMetastore createFileHiveMetastore(String catalogDir)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of(), hiveClientConfig);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, catalogDir, "test");
    }

    private static Column column(String name, HiveType type)
    {
        return new Column(name, type, Optional.empty(), Optional.empty());
    }

    private static void registerHudiTableInHMS(HoodieTableType type, String name, Path dataDir, List<Column> dataColumns)
    {
        // ref: org.apache.hudi.hive.ddl.HMSDDLExecutor#createTable
        Table table = Table.builder()
                .setDatabaseName(HUDI_SCHEMA)
                .setTableName(name)
                .setTableType(PrestoTableType.EXTERNAL_TABLE)
                .setOwner(OWNER_PUBLIC)
                .setDataColumns(dataColumns)
                .setPartitionColumns(PARTITION_COLUMNS)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(buildingStorage(type, getTablePath(name, dataDir)))
                .build();
        metastore.createTable(METASTORE_CONTEXT, table, PRINCIPAL_PRIVILEGES);
    }

    private static Consumer<Storage.Builder> buildingStorage(HoodieTableType tableType, String location)
    {
        return storageBuilder -> storageBuilder.setStorageFormat(getStorageFormat(tableType)).setLocation(location);
    }

    private static StorageFormat getStorageFormat(HoodieTableType tableType)
    {
        if (tableType == HoodieTableType.COPY_ON_WRITE) {
            return STORAGE_FORMAT_COPY_ON_WRITE;
        }
        if (tableType == HoodieTableType.MERGE_ON_READ) {
            return STORAGE_FORMAT_MERGE_ON_READ;
        }
        throw new IllegalArgumentException("Unsupported table type " + tableType);
    }

    private static void unzip(InputStream inputStream, Path destination)
            throws IOException
    {
        createDirectories(destination);
        try (ZipInputStream zipStream = new ZipInputStream(inputStream)) {
            while (true) {
                ZipEntry zipEntry = zipStream.getNextEntry();
                if (zipEntry == null) {
                    break;
                }

                Path entryPath = destination.resolve(zipEntry.getName());
                if (zipEntry.isDirectory()) {
                    createDirectories(entryPath);
                }
                else {
                    createDirectories(entryPath.getParent());
                    Files.copy(zipStream, entryPath, REPLACE_EXISTING);
                }
                zipStream.closeEntry();
            }
        }
    }
}
