package com.facebook.presto.hudi.testing;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.thrift.HiveMetastore;
import com.facebook.presto.hudi.HudiConnector;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.hive.HiveType.HIVE_BINARY;
import static com.facebook.presto.hive.HiveType.HIVE_BOOLEAN;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.HIVE_TIMESTAMP;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;

public class ResourceHudiTablesInitializer
        implements HudiTablesInitializer
{
    private static final Logger log = Logger.get(ResourceHudiTablesInitializer.class);
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final String TEST_RESOURCE_NAME = "hudi-testing-data";

    // There might be other entry points that are using this initializer, make the location unique so it is more identifiable via logs
    private final String baseLocationPrefix = UUID.randomUUID().toString();
    private final Path tempDir;

    /**
     * Manually declaring a temp directory here and performing a manual cleanup as this constructor is invoked in HudiQueryRunner in a @BeforeAll static function.
     * This means that jupiter's managed @TempDir annotation cannot be used as the path will be passed as null.
     */
    public ResourceHudiTablesInitializer()
    {
        // There are multiple entry points and they may perform unzipping together, ensure that they are all unzipping to different paths
        try {
            this.tempDir = Files.createTempDirectory(TEST_RESOURCE_NAME + "_" + baseLocationPrefix);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initializeTables(QueryRunner queryRunner, org.apache.hadoop.fs.Path externalLocation, String schemaName)
            throws Exception
    {
        // Inflate all deflated test resource archives to a temporary directory
        HudiTableUnzipper.unzipAllItemsInResource(TEST_RESOURCE_NAME, tempDir);
        ExtendedFileSystem fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        String locationSuffix = schemaName + "_" + baseLocationPrefix;
        Path baseLocation = externalLocation.appendSuffix(locationSuffix);
        log.info("Initialized test resource directory as: %s", baseLocation.toString());
        copyDir(tempDir, fileSystem, baseLocation);
        // Perform a cleanup
        HudiTableUnzipper.deleteInflatedFiles(tempDir);

        for (TestingTable table : TestingTable.values()) {
            String tableName = table.getTableName();
            Path tablePath = baseLocation.appendPath(tableName);

            // Always create ro table by default
            createTable(
                    queryRunner,
                    schemaName,
                    tablePath,
                    tableName,
                    table.getDataColumns(),
                    table.getPartitionColumns(),
                    table.getPartitions(),
                    false);

            if (table.isCreateRtTable()) {
                createTable(
                        queryRunner,
                        schemaName,
                        tablePath,
                        table.getRtTableName(),
                        table.getDataColumns(),
                        table.getPartitionColumns(),
                        table.getPartitions(),
                        true);
            }

            // Set table version
            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                    .setStorage(new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration()))
                    .setBasePath(tablePath.toString())
                    .build();
            table.setTableVersion(metaClient.getTableConfig().getTableVersion());
        }
    }

    private void createTable(
            QueryRunner queryRunner,
            String schemaName,
            Path tablePath,
            String tableName,
            List<Column> dataColumns,
            List<Column> partitionColumns,
            Map<String, String> partitions,
            boolean isRtTable)
    {
        StorageFormat roStorageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        StorageFormat rtStorageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_REALTIME_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        Table table = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(dataColumns)
                .setPartitionColumns(partitionColumns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(isRtTable ? rtStorageFormat : roStorageFormat)
                        .setLocation(tablePath.toString()))
                .build();

        HiveMetastore metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        metastore.createTable(table, PrincipalPrivileges.NO_PRIVILEGES);

        List<PartitionWithStatistics> partitionsToAdd = new ArrayList<>();
        partitions.forEach((partitionName, partitionPath) -> {
            Partition partition = Partition.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(tableName)
                    .setValues(extractPartitionValues(partitionName))
                    .withStorage(storageBuilder -> storageBuilder
                            .setStorageFormat(isRtTable ? rtStorageFormat : roStorageFormat)
                            .setLocation(tablePath.appendPath(partitionPath).toString()))
                    .setColumns(dataColumns)
                    .build();
            partitionsToAdd.add(new PartitionWithStatistics(partition, partitionName, PartitionStatistics.empty()));
        });
        metastore.addPartitions(schemaName, tableName, partitionsToAdd);
    }

    private static Column column(String name, HiveType type)
    {
        return new Column(name, type, Optional.empty(), Map.of());
    }

    public static void copyDir(Path sourceDirectory, ExtendedFileSystem fileSystem, Path destinationDirectory)
            throws IOException
    {
        try (Stream<Path> paths = Files.walk(sourceDirectory)) {
            for (Iterator<Path> iterator = paths.iterator(); iterator.hasNext(); ) {
                Path path = iterator.next();
                if (path.toFile().isDirectory()) {
                    continue;
                }

                // hudi blows up if crc files are present
                if (path.toString().endsWith(".crc")) {
                    continue;
                }

                HashAndSizeResult srcHashAndSize;
                try {
                    srcHashAndSize = calculateHashAndSize(path);
                }
                catch (NoSuchAlgorithmException e) {
                    throw new IOException("Failed to calculate source hash: Algorithm not found", e);
                }

                Path location = destinationDirectory.appendPath(sourceDirectory.relativize(path).toString());
                fileSystem.createDirectory(location.parentDirectory());
                try (OutputStream out = fileSystem.newOutputFile(location).create()) {
                    Files.copy(path, out);
                    // Flush all data before close() to ensure durability
                    out.flush();
                }

                HashAndSizeResult dstHashAndSize;
                try {
                    dstHashAndSize = calculateHashAndSize(location, fileSystem);
                }
                catch (NoSuchAlgorithmException e) {
                    throw new IOException("Failed to calculate destination hash: Algorithm not found", e);
                }
                catch (IOException e) {
                    throw new IOException("Failed to read back " + location + " for hash verification", e);
                }

                if (!Arrays.equals(srcHashAndSize.hash, dstHashAndSize.hash)) {
                    // Hashes do not match, file is corrupt or copy failed
                    String errorMessage = String.format(
                            "Hash mismatch for file: %s (source size: %d bytes) copied to %s (destination size: %d bytes). Content hashes differ",
                            path,
                            srcHashAndSize.size,
                            location,
                            dstHashAndSize.size);
                    throw new IOException(errorMessage);
                }
            }
        }
    }

    /**
     * Helper method to calculate hash and size from an input stream
     */
    private static HashAndSizeResult calculateHashAndSize(InputStream inputStream)
            throws IOException, NoSuchAlgorithmException
    {
        MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
        try (DigestInputStream dis = new DigestInputStream(inputStream, md)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            long streamSize = 0;
            while ((bytesRead = dis.read(buffer)) != -1) {
                streamSize += bytesRead;
            }
            return new HashAndSizeResult(md.digest(), streamSize);
        }
    }

    /**
     * Helper method to calculate hash for a local Path
     */
    private static HashAndSizeResult calculateHashAndSize(Path path)
            throws IOException, NoSuchAlgorithmException
    {
        try (InputStream is = Files.newInputStream(path)) {
            return calculateHashAndSize(is);
        }
    }

    /**
     * Helper method to calculate hash for a file on TrinoFileSystem
     */
    private static HashAndSizeResult calculateHashAndSize(Path location, ExtendedFileSystem fileSystem)
            throws IOException, NoSuchAlgorithmException
    {
        try (InputStream is = fileSystem.newInputFile(location).newStream()) {
            return calculateHashAndSize(is);
        }
    }

    public enum TestingTable
    {
        HUDI_NON_PART_COW(nonPartitionRegularColumns()),
        HUDI_NON_PART_MOR(simpleRegularColumns(), ImmutableList.of(), ImmutableMap.of(), true),
        HUDI_TRIPS_COW_V8(tripsRegularColumns()),
        HUDI_COW_TABLE_WITH_FIELD_NAMES_IN_CAPS(hudiTableWithFieldNamesInCapsRegularColumns()),
        HUDI_COW_PT_TABLE_WITH_FIELD_NAMES_IN_CAPS(hudiTableWithFieldNamesInCapsRegularColumns(), hudiTableWithFieldNamesInCapsPartitionColumns(), hudiTableWithFieldNamesInCapsPartitions(), false), // delete
        HUDI_COW_TABLE_WITH_MULTI_KEYS_AND_FIELD_NAMES_IN_CAPS(hudiTableWithFieldNamesInCapsRegularColumns()),
        HUDI_MOR_TABLE_WITH_FIELD_NAMES_IN_CAPS(hudiTableWithFieldNamesInCapsRegularColumns(), ImmutableList.of(), ImmutableMap.of(), true),
        HUDI_COW_PT_TBL(multiPartitionRegularColumns(), multiPartitionColumns(), multiPartitions(), false),
        STOCK_TICKS_COW(stockTicksRegularColumns(), stockTicksPartitionColumns(), stockTicksPartitions(), false),
        STOCK_TICKS_MOR(stockTicksRegularColumns(), stockTicksPartitionColumns(), stockTicksPartitions(), false),
        HUDI_STOCK_TICKS_COW(hudiStockTicksRegularColumns(), hudiStockTicksPartitionColumns(), hudiStockTicksPartitions(), false),
        HUDI_STOCK_TICKS_MOR(hudiStockTicksRegularColumns(), hudiStockTicksPartitionColumns(), hudiStockTicksPartitions(), false),
        HUDI_MULTI_FG_PT_V6_MOR(hudiMultiFgRegularColumns(), hudiMultiFgPartitionsColumn(), hudiMultiFgPartitions(), false),
        HUDI_MULTI_FG_PT_V8_MOR(hudiMultiFgRegularColumns(), hudiMultiFgPartitionsColumn(), hudiMultiFgPartitions(), false),
        HUDI_COMPREHENSIVE_TYPES_V6_MOR(hudiComprehensiveTypesColumns(), hudiComprehensiveTypesPartitionColumns(), hudiComprehensiveTypesPartitions(), true),
        HUDI_COMPREHENSIVE_TYPES_V8_MOR(hudiComprehensiveTypesColumns(), hudiComprehensiveTypesPartitionColumns(), hudiComprehensiveTypesPartitions(), true),
        HUDI_MULTI_PT_V8_MOR(hudiMultiPtMorColumns(), hudiMultiPtMorPartitionColumns(), hudiMultiPtMorPartitions(), false),
        HUDI_TIMESTAMP_KEYGEN_PT_EPOCH_TO_YYYY_MM_DD_HH_V8_MOR(hudiTimestampKeygenColumns(), hudiTimestampKeygenPartitionColumns(), hudiTimestampKeygenPartitions("EPOCHMILLISECONDS"), true),
        HUDI_TIMESTAMP_KEYGEN_PT_SCALAR_TO_YYYY_MM_DD_HH_V8_MOR(hudiTimestampKeygenColumns(), hudiTimestampKeygenPartitionColumns(), hudiTimestampKeygenPartitions("SCALAR"), true),
        HUDI_CUSTOM_KEYGEN_PT_V8_MOR(hudiCustomKeyGenColumns(), hudiCustomKeyGenPartitionColumns(), hudiCustomKeyGenPartitions(), false),
        HUDI_NON_EXTRACTABLE_PARTITION_PATH(multiPartitionRegularColumns(), multiPartitionColumns(), multiPartitionsWithNonExtractablePartitionPaths(), false),
        /**/;

        private static final List<Column> HUDI_META_COLUMNS = ImmutableList.of(
                new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_record_key", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("_hoodie_file_name", HIVE_STRING, Optional.empty(), Map.of()));

        private final List<Column> regularColumns;
        private final List<Column> partitionColumns;
        private final Map<String, String> partitions;
        private final boolean isCreateRtTable;
        private HoodieTableVersion tableVersion;

        TestingTable(
                List<Column> regularColumns,
                List<Column> partitionColumns,
                Map<String, String> partitions,
                boolean isCreateRtTable)
        {
            this.regularColumns = regularColumns;
            this.partitionColumns = partitionColumns;
            this.partitions = partitions;
            this.isCreateRtTable = isCreateRtTable;
        }

        TestingTable(List<Column> regularColumns)
        {
            this(regularColumns, ImmutableList.of(), ImmutableMap.of(), false);
        }

        public String getTableName()
        {
            return name().toLowerCase(Locale.ROOT);
        }

        public String getRtTableName()
        {
            return name().toLowerCase(Locale.ROOT) + "_rt";
        }

        public String getRoTableName()
        {
            // ro tables do not have suffix
            return getTableName();
        }

        public void setTableVersion(HoodieTableVersion tableVersion)
        {
            this.tableVersion = tableVersion;
        }

        public HoodieTableVersion getHoodieTableVersion()
        {
            return this.tableVersion;
        }

        public List<Column> getDataColumns()
        {
            return Stream.of(HUDI_META_COLUMNS, regularColumns)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toUnmodifiableList());
        }

        public List<Column> getPartitionColumns()
        {
            return partitionColumns;
        }

        public Map<String, String> getPartitions()
        {
            return partitions;
        }

        public boolean isCreateRtTable()
        {
            return isCreateRtTable;
        }

        private static List<Column> nonPartitionRegularColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_LONG),
                    column("name", HIVE_STRING),
                    column("ts", HIVE_LONG),
                    column("dt", HIVE_STRING),
                    column("hh", HIVE_STRING));
        }

        // Table schema has capitalized column names, but the catalog returns them in lowercase.
        // Using lowercase here to match the catalog for testing.
        private static List<Column> hudiTableWithFieldNamesInCapsRegularColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_STRING),
                    column("name", HIVE_STRING),
                    column("age", HIVE_INT));
        }

        // The actual Hudi table has "Country" as the partition field name, but the catalog provides it in lowercase.
        // Using lowercase here to stay consistent with the catalog for testing.
        private static Map<String, String> hudiTableWithFieldNamesInCapsPartitions()
        {
            return ImmutableMap.of(
                    "country=IND", "IND",
                    "country=US", "US");
        }

        // The actual Hudi table has "Country" as the partition field name, but the catalog provides it in lowercase.
        // Using lowercase here to stay consistent with the catalog for testing.
        private static List<Column> hudiTableWithFieldNamesInCapsPartitionColumns()
        {
            return ImmutableList.of(column("country", HIVE_STRING));
        }

        private static List<Column> simpleRegularColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_STRING),
                    column("name", HIVE_STRING),
                    column("age", HIVE_INT));
        }

        private static List<Column> tripsRegularColumns()
        {
            return ImmutableList.of(
                    column("begin_lat", HIVE_DOUBLE),
                    column("begin_lon", HIVE_DOUBLE),
                    column("driver", HIVE_STRING),
                    column("end_lat", HIVE_DOUBLE),
                    column("end_lon", HIVE_DOUBLE),
                    column("fare", HIVE_DOUBLE),
                    column("partitionpath", HIVE_STRING),
                    column("rider", HIVE_STRING),
                    column("ts", HIVE_LONG),
                    column("uuid", HIVE_STRING));
        }

        private static List<Column> stockTicksRegularColumns()
        {
            return ImmutableList.of(
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
        }

        private static List<Column> stockTicksPartitionColumns()
        {
            return ImmutableList.of(column("dt", HIVE_STRING));
        }

        private static Map<String, String> stockTicksPartitions()
        {
            return ImmutableMap.of("dt=2018-08-31", "2018/08/31");
        }

        private static List<Column> hudiStockTicksRegularColumns()
        {
            return ImmutableList.of(
                    column("volume", HIVE_LONG),
                    column("ts", HIVE_STRING),
                    column("symbol", HIVE_STRING),
                    column("year", HIVE_INT),
                    column("month", HIVE_STRING),
                    column("high", HIVE_DOUBLE),
                    column("low", HIVE_DOUBLE),
                    column("key", HIVE_STRING),
                    column("close", HIVE_DOUBLE),
                    column("open", HIVE_DOUBLE),
                    column("day", HIVE_STRING));
        }

        private static List<Column> hudiStockTicksPartitionColumns()
        {
            return ImmutableList.of(column("date", HIVE_STRING));
        }

        private static Map<String, String> hudiStockTicksPartitions()
        {
            return ImmutableMap.of("date=2018-08-31", "2018/08/31");
        }

        private static List<Column> multiPartitionRegularColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_LONG),
                    column("name", HIVE_STRING),
                    column("ts", HIVE_LONG));
        }

        private static List<Column> multiPartitionColumns()
        {
            return ImmutableList.of(
                    column("dt", HIVE_STRING),
                    column("hh", HIVE_STRING));
        }

        private static Map<String, String> multiPartitions()
        {
            return ImmutableMap.of(
                    "dt=2021-12-09/hh=10", "dt=2021-12-09/hh=10",
                    "dt=2021-12-09/hh=11", "dt=2021-12-09/hh=11");
        }

        /**
         * Returns a sample map of partition specs containing multiple partition keys separated by slashes.
         *
         * Example:
         *   "dt=2018-10-05/hh=10" -> "2018/10/05/10"
         *
         * Note:
         *  - The partition spec has 2 partition keys (dt, hh).
         *  - However, the corresponding value string has 4 segments when split by slashes
         *    (year, month, day, hour).
         *  - Standard Hudi partition extractors will not be able to correctly parse this mapping,
         *    since they expect the number of slash-separated values to match the number of partition keys
         *    if there's more than one partition field.
         */
        private static Map<String, String> multiPartitionsWithNonExtractablePartitionPaths()
        {
            return ImmutableMap.of(
                    "dt=2018-10-05/hh=10", "2018/10/05/10",
                    "dt=2018-10-06/hh=5", "2018/10/06/5");
        }

        private static List<Column> hudiMultiFgRegularColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_INT),
                    column("name", HIVE_STRING),
                    column("price", HIVE_DOUBLE),
                    column("ts", HIVE_LONG));
        }

        private static List<Column> hudiMultiFgPartitionsColumn()
        {
            return ImmutableList.of(
                    column("country", HIVE_STRING));
        }

        private static Map<String, String> hudiMultiFgPartitions()
        {
            return ImmutableMap.of(
                    "country=SG", "country=SG",
                    "country=US", "country=US");
        }

        private static List<Column> hudiComprehensiveTypesColumns()
        {
            return ImmutableList.of(
                    // ----- Primary Key & Precombine -----
                    column("uuid", HIVE_STRING),
                    column("precombine_field", HIVE_LONG),

                    // ----- Numeric Types -----
                    column("col_boolean", HIVE_BOOLEAN),
                    column("col_tinyint", HIVE_BYTE),
                    column("col_smallint", HIVE_SHORT),
                    column("col_int", HIVE_INT),
                    column("col_bigint", HIVE_LONG),
                    column("col_float", HIVE_FLOAT),
                    column("col_double", HIVE_DOUBLE),
                    column("col_decimal", decimalHiveType(10, 2)),

                    // ----- String Types -----
                    column("col_string", HIVE_STRING),
                    column("col_varchar", varcharHiveType(50)),
                    column("col_char", charHiveType(10)),

                    // ----- Binary Type -----
                    column("col_binary", HIVE_BINARY),

                    // ----- Datetime Types -----
                    column("col_date", HIVE_DATE),
                    column("col_timestamp", HIVE_TIMESTAMP),

                    // ----- Complex Types -----
                    // ARRAY<INT>
                    column("col_array_int", listHiveType(INT_TYPE_INFO)),
                    // ARRAY<STRING>
                    column("col_array_string", listHiveType(STRING_TYPE_INFO)),
                    // MAP<STRING, INT>
                    column("col_map_string_int", mapHiveType(STRING_TYPE_INFO, INT_TYPE_INFO)),
                    // STRUCT<f1: STRING, f2: INT, f3: BOOLEAN>
                    column("col_struct", structHiveType(
                            ImmutableList.of("f1", "f2", "f3"),
                            ImmutableList.of(STRING_TYPE_INFO, INT_TYPE_INFO, BOOLEAN_TYPE_INFO))),
                    // ARRAY<STRUCT<nested_f1: DOUBLE, nested_f2: ARRAY<STRING>>>
                    column("col_array_struct", listHiveType(
                            getStructTypeInfo(
                                    ImmutableList.of("nested_f1", "nested_f2"),
                                    ImmutableList.of(DOUBLE_TYPE_INFO, ARRAY_STRING_TYPE_INFO)))),
                    // MAP<STRING, STRUCT<nested_f3: DATE, nested_f4: DECIMAL(5,2)>>
                    column("col_map_string_struct", mapHiveType(
                            STRING_TYPE_INFO,
                            getStructTypeInfo(
                                    ImmutableList.of("nested_f3", "nested_f4"),
                                    ImmutableList.of(DATE_TYPE_INFO, getDecimalTypeInfo(5, 2))))),
                    // ARRAY<STRUCT<f_arr_struct_str: STRING, f_arr_struct_map: MAP<STRING, INT>>>
                    column("col_array_struct_with_map", listHiveType(
                            getStructTypeInfo(
                                    ImmutableList.of("f_arr_struct_str", "f_arr_struct_map"),
                                    ImmutableList.of(STRING_TYPE_INFO, MAP_STRING_INT_TYPE_INFO)))),
                    // MAP<STRING, STRUCT<f_map_struct_arr: ARRAY<BOOLEAN>, f_map_struct_ts: TIMESTAMP>>
                    column("col_map_struct_with_array", mapHiveType(
                            STRING_TYPE_INFO,
                            getStructTypeInfo(
                                    ImmutableList.of("f_map_struct_arr", "f_map_struct_ts"),
                                    ImmutableList.of(ARRAY_BOOLEAN_TYPE_INFO, TIMESTAMP_TYPE_INFO)))),
                    // STRUCT<outer_f1: INT, nested_struct: STRUCT<inner_f1: STRING, inner_f2: BOOLEAN>>
                    column("col_struct_nested_struct", structHiveType(
                            ImmutableList.of("outer_f1", "nested_struct"),
                            ImmutableList.of(
                                    INT_TYPE_INFO,
                                    getStructTypeInfo(
                                            ImmutableList.of("inner_f1", "inner_f2"),
                                            ImmutableList.of(STRING_TYPE_INFO, BOOLEAN_TYPE_INFO))))),
                    // ARRAY<ARRAY<INT>>
                    column("col_array_array_int", listHiveType(ARRAY_INT_TYPE_INFO)),
                    // MAP<STRING, ARRAY<DOUBLE>>
                    column("col_map_string_array_double", mapHiveType(STRING_TYPE_INFO, ARRAY_DOUBLE_TYPE_INFO)),
                    // MAP<STRING, MAP<STRING, DATE>>
                    column("col_map_string_map_string_date", mapHiveType(STRING_TYPE_INFO, MAP_STRING_DATE_TYPE_INFO)),
                    // STRUCT<outer_f2: STRING, struct_array: ARRAY<STRUCT<inner_f3: TIMESTAMP, inner_f4: STRING>>>
                    column("col_struct_array_struct", structHiveType(
                            ImmutableList.of("outer_f2", "struct_array"),
                            ImmutableList.of(
                                    STRING_TYPE_INFO,
                                    getListTypeInfo(getStructTypeInfo(
                                            ImmutableList.of("inner_f3", "inner_f4"),
                                            ImmutableList.of(TIMESTAMP_TYPE_INFO, STRING_TYPE_INFO)))))),
                    // STRUCT<outer_f3: BOOLEAN, struct_map: MAP<STRING, BIGINT>>
                    column("col_struct_map", structHiveType(
                            ImmutableList.of("outer_f3", "struct_map"),
                            ImmutableList.of(BOOLEAN_TYPE_INFO, MAP_STRING_LONG_TYPE_INFO))));
        }

        private static List<Column> hudiComprehensiveTypesPartitionColumns()
        {
            return ImmutableList.of(column("part_col", HIVE_STRING));
        }

        private static Map<String, String> hudiComprehensiveTypesPartitions()
        {
            return ImmutableMap.of(
                    "part_col=A", "part_col=A",
                    "part_col=B", "part_col=B");
        }

        private static List<Column> hudiMultiPtMorColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_INT),
                    column("name", HIVE_STRING),
                    column("price", HIVE_DOUBLE),
                    column("ts", HIVE_LONG));
        }

        private static List<Column> hudiMultiPtMorPartitionColumns()
        {
            return ImmutableList.of(
                    column("part_str", HIVE_STRING),
                    column("part_int", HIVE_INT),
                    column("part_date", HIVE_DATE),
                    column("part_bigint", HIVE_LONG),
                    column("part_decimal", decimalHiveType(10, 2)),
                    column("part_timestamp", HIVE_TIMESTAMP),
                    column("part_bool", HIVE_BOOLEAN));
        }

        private static Map<String, String> hudiMultiPtMorPartitions()
        {
            return ImmutableMap.of(
                    "part_str=apparel/part_int=2024/part_date=2024-01-05/part_bigint=20000000001/part_decimal=100.00/part_timestamp=2024-01-05 18%3A00%3A00/part_bool=false", "part_str=apparel/part_int=2024/part_date=2024-01-05/part_bigint=20000000001/part_decimal=100.00/part_timestamp=2024-01-05 18%3A00%3A00/part_bool=false",
                    "part_str=electronics/part_int=2023/part_date=2023-03-10/part_bigint=10000000002/part_decimal=50.00/part_timestamp=2023-03-10 12%3A30%3A00/part_bool=true", "part_str=electronics/part_int=2023/part_date=2023-03-10/part_bigint=10000000002/part_decimal=50.00/part_timestamp=2023-03-10 12%3A30%3A00/part_bool=true",
                    "part_str=electronics/part_int=2023/part_date=2023-03-10/part_bigint=10000000002/part_decimal=50.00/part_timestamp=2023-03-10 12%3A30%3A00/part_bool=false", "part_str=electronics/part_int=2023/part_date=2023-03-10/part_bigint=10000000002/part_decimal=50.00/part_timestamp=2023-03-10 12%3A30%3A00/part_bool=false",
                    "part_str=books/part_int=2023/part_date=2023-01-15/part_bigint=10000000001/part_decimal=123.00/part_timestamp=2023-01-15 10%3A00%3A00/part_bool=true", "part_str=books/part_int=2023/part_date=2023-01-15/part_bigint=10000000001/part_decimal=123.00/part_timestamp=2023-01-15 10%3A00%3A00/part_bool=true",
                    "part_str=books/part_int=2024/part_date=2024-02-20/part_bigint=10000000003/part_decimal=75.00/part_timestamp=2024-02-20 08%3A45%3A10/part_bool=true", "part_str=books/part_int=2024/part_date=2024-02-20/part_bigint=10000000003/part_decimal=75.00/part_timestamp=2024-02-20 08%3A45%3A10/part_bool=true");
        }

        private static List<Column> hudiTimestampKeygenColumns()
        {
            return ImmutableList.of(
                    column("id", HIVE_INT),
                    column("name", HIVE_STRING),
                    column("price", HIVE_DOUBLE),
                    column("ts", HIVE_LONG));
        }

        private static List<Column> hudiTimestampKeygenPartitionColumns()
        {
            // Data stored in files are long, but partition value that is synced to metastore is String
            return ImmutableList.of(column("partition_field", HIVE_STRING));
        }

        private static Map<String, String> hudiTimestampKeygenPartitions(String timestampType)
        {
            return switch (timestampType) {
                case "EPOCHMILLISECONDS" -> ImmutableMap.of(
                        "partition_field=2025-05-13 02", "2025-05-13 02",
                        "partition_field=2025-06-05 05", "2025-06-05 05",
                        "partition_field=2025-06-06 09", "2025-06-06 09",
                        "partition_field=2025-06-06 10", "2025-06-06 10",
                        "partition_field=2025-06-07 08", "2025-06-07 08");
                case "SCALAR" -> ImmutableMap.of(
                        "partition_field=2024-10-08 12", "2024-10-08 12",
                        "partition_field=2024-10-07 12", "2024-10-07 12",
                        "partition_field=2024-10-06 12", "2024-10-06 12",
                        "partition_field=2024-10-05 12", "2024-10-05 12",
                        "partition_field=2024-10-04 12", "2024-10-04 12");
                default -> ImmutableMap.of();
            };
        }

        private static List<Column> hudiCustomKeyGenColumns()
        {
            return hudiMultiFgRegularColumns();
        }

        private static List<Column> hudiCustomKeyGenPartitionColumns()
        {
            return ImmutableList.of(
                    column("partition_field_country", HIVE_STRING),
                    column("partition_field_date", HIVE_STRING));
        }

        private static Map<String, String> hudiCustomKeyGenPartitions()
        {
            return ImmutableMap.of(
                    "partition_field_country=US/partition_field_date=2025-06-06", "partition_field_country=US/partition_field_date=2025-06-06",
                    "partition_field_country=CN/partition_field_date=2025-06-05", "partition_field_country=CN/partition_field_date=2025-06-05",
                    "partition_field_country=MY/partition_field_date=2025-05-13", "partition_field_country=MY/partition_field_date=2025-05-13",
                    "partition_field_country=SG/partition_field_date=2025-06-06", "partition_field_country=SG/partition_field_date=2025-06-06",
                    "partition_field_country=SG/partition_field_date=2025-06-07", "partition_field_country=SG/partition_field_date=2025-06-07");
        }
    }

    static class HashAndSizeResult
    {
        final byte[] hash;
        final long size;

        HashAndSizeResult(byte[] hash, long size)
        {
            this.hash = hash;
            this.size = size;
        }
    }
}