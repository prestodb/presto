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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hudi.common.LatestHudiColumnStatsReader;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.MoreFiles;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.hadoop.HoodieAvroHFileWriter;
import org.apache.hudi.io.storage.HoodieHFileConfig;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.org.apache.avro.generic.GenericRecord;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.getAllSessionProperties;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.TestHudiDirectoryLister.getHadoopConfWithCopyOnFirstWriteDisabled;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.io.compress.CompressionCodec.NONE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestHudiDataSkipping
{
    private static final String INSTANT = "20260101000000000";
    private static final Schema DATA_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"data\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"},{\"name\":\"y\",\"type\":\"long\"}]}");
    private static final Schema METADATA_SCHEMA = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());

    @DataProvider
    public Object[][] listingModes()
    {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "listingModes")
    public void testRealMetadataPrunesCowFiles(boolean metadataListing)
            throws Exception
    {
        try (TestTable table = new TestTable(MetadataState.COLUMN_STATS)) {
            Map<Pair<String, String>, Map<String, Domain>> statistics = new LatestHudiColumnStatsReader(table.metaClient)
                    .readColumnStats(List.of(Pair.of("", table.lowFile), Pair.of("", table.highFile), Pair.of("", table.unindexedFile)), Map.of("x", BIGINT));
            assertEquals(statistics.get(Pair.of("", table.lowFile)).get("x"), Domain.singleValue(BIGINT, 1L));
            assertEquals(statistics.get(Pair.of("", table.highFile)).get("x"), Domain.singleValue(BIGINT, 100L));
            assertEquals(statistics.get(Pair.of("", table.unindexedFile)), Map.of());
            assertEquals(new LatestHudiColumnStatsReader(table.metaClient)
                    .readColumnStats(List.of(Pair.of("", table.highFile)), Map.of("x", INTEGER)), Map.of());

            TupleDomain<String> predicate = TupleDomain.withColumnDomains(Map.of("x", Domain.singleValue(BIGINT, 100L)));
            List<String> baseline = listFiles(table, metadataListing, false, true, predicate);
            List<String> pruned = listFiles(table, metadataListing, true, true, predicate);
            assertEquals(baseline, List.of(table.lowFile, table.highFile, table.unindexedFile));
            assertEquals(pruned, List.of(table.highFile, table.unindexedFile));
            assertEquals(readMatchingRows(table, baseline), List.of(100L, 100L));
            assertEquals(readMatchingRows(table, pruned), readMatchingRows(table, baseline));
            // Positional Parquet reads are excluded because index fields are identified by name.
            assertEquals(listFiles(table, metadataListing, true, false, predicate).size(), 3);
            assertEquals(listFiles(table, metadataListing, true, true, TupleDomain.all()).size(), 3);
        }
    }

    @DataProvider
    public Object[][] absentIndexes()
    {
        return new Object[][] {
                {MetadataState.NONE, false},
                {MetadataState.NONE, true},
                {MetadataState.FILES_ONLY, false},
                {MetadataState.FILES_ONLY, true},
                {MetadataState.COLUMN_STATS_INFLIGHT, false},
                {MetadataState.COLUMN_STATS_INFLIGHT, true}};
    }

    @Test(dataProvider = "absentIndexes")
    public void testNoUsableIndexDoesNotPrune(MetadataState metadataState, boolean metadataListing)
            throws Exception
    {
        try (TestTable table = new TestTable(metadataState)) {
            TupleDomain<String> predicate = TupleDomain.withColumnDomains(Map.of("x", Domain.singleValue(BIGINT, 100L)));
            RuntimeStats stats = new RuntimeStats();
            List<String> files = listFiles(table, metadataListing, true, true, predicate, stats);
            assertEquals(files, List.of(table.lowFile, table.highFile, table.unindexedFile));
            assertEquals(readMatchingRows(table, files), List.of(100L, 100L));
            assertEquals(new LatestHudiColumnStatsReader(table.metaClient)
                    .readColumnStats(List.of(Pair.of("", table.highFile)), Map.of("x", BIGINT)), Map.of());
            // No index is a normal case, not a failed statistics lookup.
            assertNull(stats.getMetric("hudiDataSkippingLookupTime"));
            assertNull(stats.getMetric("hudiDataSkippingFallbacks"));
        }
    }

    @Test(dataProvider = "listingModes")
    public void testUnindexedColumnDoesNotExcludeFiles(boolean metadataListing)
            throws Exception
    {
        try (TestTable table = new TestTable(MetadataState.COLUMN_STATS)) {
            Domain matches = Domain.singleValue(BIGINT, 100L);
            assertEquals(listFiles(table, metadataListing, true, true, TupleDomain.withColumnDomains(Map.of("y", matches))),
                    List.of(table.lowFile, table.highFile, table.unindexedFile));
            // The indexed conjunct can still rule out a file; the missing conjunct cannot.
            assertEquals(listFiles(table, metadataListing, true, true, TupleDomain.withColumnDomains(Map.of("x", matches, "y", matches))),
                    List.of(table.highFile, table.unindexedFile));
        }
    }

    @Test(dataProvider = "listingModes")
    public void testAdvertisedButUnavailableIndexRetainsFiles(boolean metadataListing)
            throws Exception
    {
        try (TestTable table = new TestTable(MetadataState.COLUMN_STATS_UNAVAILABLE)) {
            TupleDomain<String> predicate = TupleDomain.withColumnDomains(Map.of("x", Domain.singleValue(BIGINT, 100L)));
            List<String> files = listFiles(table, metadataListing, true, true, predicate);
            assertEquals(files, List.of(table.lowFile, table.highFile, table.unindexedFile));
            assertEquals(readMatchingRows(table, files), List.of(100L, 100L));
        }
    }

    private static List<Long> readMatchingRows(TestTable table, List<String> files)
            throws IOException
    {
        List<Long> rows = new ArrayList<>();
        for (String file : files) {
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(table.location + file))
                    .withConf(table.configuration).build()) {
                Group row;
                while ((row = reader.read()) != null) {
                    long value = row.getLong("x", 0);
                    if (value == 100) {
                        rows.add(value);
                    }
                }
            }
        }
        return rows;
    }

    private static List<String> listFiles(
            TestTable testTable,
            boolean metadataListing,
            boolean skipping,
            boolean columnNames,
            TupleDomain<String> predicate)
            throws IOException
    {
        return listFiles(testTable, metadataListing, skipping, columnNames, predicate, new RuntimeStats());
    }

    private static List<String> listFiles(
            TestTable testTable,
            boolean metadataListing,
            boolean skipping,
            boolean columnNames,
            TupleDomain<String> predicate,
            RuntimeStats stats)
            throws IOException
    {
        ConnectorSession session = new TestingConnectorSession(getAllSessionProperties(
                new HiveClientConfig().setHudiMetadataEnabled(metadataListing).setHudiMdtDataSkippingEnabled(skipping),
                new HiveCommonClientConfig().setUseParquetColumnNames(columnNames)));
        HudiDirectoryLister lister = new HudiDirectoryLister(testTable.configuration, session, testTable.table, predicate, HDFS_ENVIRONMENT);
        Path path = new Path(testTable.location);
        ExtendedFileSystem fs = (ExtendedFileSystem) path.getFileSystem(testTable.configuration);
        List<HiveFileInfo> files = ImmutableList.copyOf(lister.list(fs, testTable.table, path, Optional.empty(), new NamenodeStats(),
                new HiveDirectoryContext(IGNORED, false, false, session.getIdentity(), ImmutableMap.of(), stats)));
        return files.stream().map(HiveFileInfo::getFileName).sorted().collect(Collectors.toList());
    }

    private enum MetadataState
    {
        NONE,
        FILES_ONLY,
        COLUMN_STATS_INFLIGHT,
        COLUMN_STATS,
        COLUMN_STATS_UNAVAILABLE
    }

    private static class TestTable
            implements AutoCloseable
    {
        private final java.nio.file.Path directory;
        private final String location;
        private final Configuration configuration = getHadoopConfWithCopyOnFirstWriteDisabled();
        private final String lowFile = "00000000-0000-0000-0000-000000000001_0-0-0_" + INSTANT + ".parquet";
        private final String highFile = "00000000-0000-0000-0000-000000000002_0-0-0_" + INSTANT + ".parquet";
        private final String unindexedFile = "00000000-0000-0000-0000-000000000003_0-0-0_" + INSTANT + ".parquet";
        private final HoodieTableMetaClient metaClient;
        private final Table table;

        TestTable(MetadataState metadataState)
                throws Exception
        {
            directory = Files.createTempDirectory("hudi-data-skipping");
            location = directory.toUri().toString();
            try {
                HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(configuration);
                metaClient = HoodieTableMetaClient.newTableBuilder()
                        .setTableName("data")
                        .setTableType(HoodieTableType.COPY_ON_WRITE)
                        .setTableVersion(HoodieTableVersion.EIGHT)
                        .setTableCreateSchema(DATA_SCHEMA.toString())
                        .setRecordKeyFields("x")
                        .setPopulateMetaFields(false)
                        .setMetadataPartitions(metadataState == MetadataState.NONE ? "" :
                                (metadataState == MetadataState.COLUMN_STATS || metadataState == MetadataState.COLUMN_STATS_UNAVAILABLE ? "files,column_stats" : "files"))
                        .setInflightMetadataPartitions(metadataState == MetadataState.COLUMN_STATS_INFLIGHT ? "column_stats" : "")
                        .initTable(storageConf, location);
                writeData(lowFile, 1L);
                writeData(highFile, 100L);
                writeData(unindexedFile, 100L);
                completeCommit(metaClient, DATA_SCHEMA);

                if (metadataState != MetadataState.NONE) {
                    String metadataPath = location + ".hoodie/metadata";
                    HoodieTableMetaClient metadata = HoodieTableMetaClient.newTableBuilder()
                            .setTableName("data_metadata")
                            .setTableType(HoodieTableType.MERGE_ON_READ)
                            .setTableVersion(HoodieTableVersion.EIGHT)
                            .setBaseFileFormat("HFILE")
                            .setPayloadClass(HoodieMetadataPayload.class)
                            .setTableCreateSchema(METADATA_SCHEMA.toString())
                            .setRecordKeyFields("key")
                            .initTable(storageConf, metadataPath);
                    Map<String, Long> lengths = Map.of(
                            lowFile, Files.size(directory.resolve(lowFile)),
                            highFile, Files.size(directory.resolve(highFile)),
                            unindexedFile, Files.size(directory.resolve(unindexedFile)));
                    writeMetadata(metadataPath, "files", "files-0000-0", List.of(
                            HoodieMetadataPayload.createPartitionListRecord(List.of("")),
                            HoodieMetadataPayload.createPartitionFilesRecord("", lengths, List.of())));
                    if (metadataState == MetadataState.COLUMN_STATS) {
                        writeMetadata(metadataPath, "column_stats", "col-stats-0000-0",
                                List.of(columnStatistics(lowFile, 1L), columnStatistics(highFile, 100L)));
                    }
                    completeCommit(metadata, METADATA_SCHEMA);
                }
                metaClient.reloadActiveTimeline();

                StorageFormat format = StorageFormat.create(
                        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "org.apache.hudi.hadoop.HoodieParquetInputFormat",
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
                table = new Table("schema", "data", "user", EXTERNAL_TABLE,
                        new Storage(format, location, Optional.empty(), false, ImmutableMap.of(), ImmutableMap.of()),
                        ImmutableList.of(
                                new Column("x", HIVE_LONG, Optional.empty(), Optional.empty()),
                                new Column("y", HIVE_LONG, Optional.empty(), Optional.empty())),
                        ImmutableList.of(), ImmutableMap.of(), Optional.empty(), Optional.empty());
            }
            catch (Exception e) {
                close();
                throw e;
            }
        }

        private void writeData(String file, long value)
                throws IOException
        {
            MessageType schema = MessageTypeParser.parseMessageType("message data { required int64 x; required int64 y; }");
            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(location + file))
                    .withType(schema).withConf(configuration).build()) {
                writer.write(new SimpleGroupFactory(schema).newGroup().append("x", value).append("y", 100L));
            }
        }

        private void writeMetadata(String basePath, String partition, String fileId, List<HoodieRecord<HoodieMetadataPayload>> records)
                throws IOException
        {
            String fileName = fileId + "_0-0-0_" + INSTANT + ".hfile";
            HoodieHFileConfig config = new HoodieHFileConfig(new HadoopStorageConfiguration(configuration),
                    NONE, 4096, 1024 * 1024, "key", null);
            try (HoodieAvroHFileWriter writer = new HoodieAvroHFileWriter(INSTANT,
                    new StoragePath(basePath + "/" + partition + "/" + fileName), config,
                    METADATA_SCHEMA, new LocalTaskContextSupplier(), false)) {
                for (HoodieRecord<HoodieMetadataPayload> record : records.stream().sorted(Comparator.comparing(HoodieRecord::getRecordKey)).collect(Collectors.toList())) {
                    GenericRecord value = (GenericRecord) record.getData().getInsertValue(METADATA_SCHEMA).get();
                    value.put("_hoodie_record_key", record.getRecordKey());
                    value.put("_hoodie_partition_path", partition);
                    value.put("_hoodie_commit_time", INSTANT);
                    value.put("_hoodie_file_name", fileName);
                    writer.writeAvro(record.getRecordKey(), value);
                }
            }
        }

        @SuppressWarnings("unchecked")
        private static HoodieRecord<HoodieMetadataPayload> columnStatistics(String file, long value)
        {
            HoodieColumnRangeMetadata range = HoodieColumnRangeMetadata.create(file, "x", value, value, 0, 1, 0, 0,
                    ValueMetadata.getEmptyValueMetadata(HoodieIndexVersion.V1));
            return (HoodieRecord<HoodieMetadataPayload>) HoodieMetadataPayload.createColumnStatsRecords("", List.of(range), false).findFirst().get();
        }

        private static void completeCommit(HoodieTableMetaClient client, Schema schema)
        {
            HoodieCommitMetadata commit = new HoodieCommitMetadata();
            commit.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schema.toString());
            HoodieInstant inflight = client.createNewInstant(HoodieInstant.State.INFLIGHT, COMMIT_ACTION, INSTANT);
            client.getActiveTimeline().createNewInstant(inflight);
            client.getActiveTimeline().saveAsComplete(inflight, Option.of(commit));
            client.reloadActiveTimeline();
        }

        @Override
        public void close()
                throws IOException
        {
            MoreFiles.deleteRecursively(directory, ALLOW_INSECURE);
        }
    }
}
