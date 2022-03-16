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

package com.facebook.presto.hive.hudi;

import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Streams;
import com.google.common.io.Resources;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;

public class HudiTestingDataGenerator
{
    private static final String OWNER_PUBLIC = "public";
    private static final MetastoreContext METASTORE_CONTEXT = new MetastoreContext("test_user", "test_queryId", Optional.empty(), Optional.empty(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
    private static final PrincipalPrivileges PRINCIPAL_PRIVILEGES = new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of());

    private static final StorageFormat STORAGE_FORMAT_COPY_ON_WRITE = StorageFormat.create(
            ParquetHiveSerDe.class.getName(),
            HoodieParquetInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName());
    private static final StorageFormat STORAGE_FORMAT_MERGE_ON_READ = StorageFormat.create(
            ParquetHiveSerDe.class.getName(),
            HoodieParquetRealtimeInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName());

    public static final List<Column> DATA_COLUMNS = ImmutableList.of(
            column("volume", HIVE_LONG),
            column("ts", HIVE_STRING),
            column("symbol", HIVE_STRING),
            column("year", HIVE_INT),
            column("month", HIVE_STRING),
            column("high", HIVE_DOUBLE),
            column("low", HIVE_DOUBLE),
            column("key", HIVE_STRING),
            column("date", HIVE_STRING),
            column("close", HIVE_DOUBLE),
            column("open", HIVE_DOUBLE),
            column("day", HIVE_STRING));
    public static final List<Column> PARTITION_COLUMNS = ImmutableList.of(column("dt", HIVE_STRING));
    public static final List<Column> HUDI_META_COLUMNS = ImmutableList.of(
            column("_hoodie_commit_time", HiveType.HIVE_STRING),
            column("_hoodie_commit_seqno", HiveType.HIVE_STRING),
            column("_hoodie_record_key", HiveType.HIVE_STRING),
            column("_hoodie_partition_path", HiveType.HIVE_STRING),
            column("_hoodie_file_name", HiveType.HIVE_STRING));

    private final Path dataDirectory;
    private final ExtendedHiveMetastore metastore;
    private final String schemaName;

    public HudiTestingDataGenerator(ExtendedHiveMetastore metastore, String schemaName, Path dataDirectory)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.schemaName = requireNonNull(schemaName, "catalogName is null");
        this.dataDirectory = requireNonNull(dataDirectory, "dataDirectory is null");
    }

    public void generateData()
    {
        // Check `hudi-testing-data.md` for more about the testing data.
        try (InputStream stream = Resources.getResource("hudi-testing-data.zip").openStream()) {
            unzip(stream, dataDirectory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void generateMetadata()
    {
        // create database
        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setOwnerName(OWNER_PUBLIC)
                .setOwnerType(PrincipalType.ROLE)
                .build();
        metastore.createDatabase(METASTORE_CONTEXT, database);

        // create partition tables
        createTable(COPY_ON_WRITE, "stock_ticks_cow", "stock_ticks_cow", true);
        addPartition(COPY_ON_WRITE, "stock_ticks_cow", ImmutableList.of("dt=2018-08-31"), "stock_ticks_cow/2018/08/31");
        createTable(COPY_ON_WRITE, "stock_ticks_mor_ro", "stock_ticks_mor", true);
        addPartition(COPY_ON_WRITE, "stock_ticks_mor_ro", ImmutableList.of("dt=2018-08-31"), "stock_ticks_mor/2018/08/31");
        createTable(MERGE_ON_READ, "stock_ticks_mor_rt", "stock_ticks_mor", true);
        addPartition(MERGE_ON_READ, "stock_ticks_mor_rt", ImmutableList.of("dt=2018-08-31"), "stock_ticks_mor/2018/08/31");

        // non partition tables
        createTable(COPY_ON_WRITE, "stock_ticks_cown", dataDirectory.resolve("stock_ticks_cown").toString(), false);
        createTable(COPY_ON_WRITE, "stock_ticks_morn_ro", dataDirectory.resolve("stock_ticks_morn").toString(), false);
        createTable(MERGE_ON_READ, "stock_ticks_morn_rt", dataDirectory.resolve("stock_ticks_morn").toString(), false);
    }

    private void createTable(HoodieTableType type, String name, String relativePath, boolean partitioned)
    {
        // ref: org.apache.hudi.hive.ddl.HMSDDLExecutor#createTable
        Table table = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(name)
                .setTableType(PrestoTableType.EXTERNAL_TABLE)
                .setOwner(OWNER_PUBLIC)
                .setDataColumns(allDataColumns())
                .setPartitionColumns(partitioned ? PARTITION_COLUMNS : ImmutableList.of())
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(buildingStorage(type, "file://" + dataDirectory.resolve(relativePath)))
                .build();
        metastore.createTable(METASTORE_CONTEXT, table, PRINCIPAL_PRIVILEGES);
    }

    private void addPartition(HoodieTableType type, String tableName, List<String> partitionNames, String relativePath)
    {
        List<PartitionWithStatistics> partitions = new ArrayList<>();
        for (String partitionName : partitionNames) {
            Partition partition = Partition.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
                    .setValues(extractPartitionValues(partitionName))
                    .withStorage(buildingStorage(type, "file://" + dataDirectory.resolve(relativePath)))
                    .setColumns(allDataColumns())
                    .setCreateTime(0)
                    .build();
            partitions.add(new PartitionWithStatistics(partition, partitionName, PartitionStatistics.empty()));
        }
        metastore.addPartitions(METASTORE_CONTEXT, schemaName, tableName, partitions);
    }

    private List<Column> allDataColumns()
    {
        return Streams.concat(HUDI_META_COLUMNS.stream(), DATA_COLUMNS.stream()).collect(Collectors.toList());
    }

    private static Column column(String name, HiveType type)
    {
        return new Column(name, type, Optional.empty(), Optional.empty());
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
