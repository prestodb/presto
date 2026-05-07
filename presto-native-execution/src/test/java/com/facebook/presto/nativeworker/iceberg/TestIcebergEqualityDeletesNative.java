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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.facebook.presto.iceberg.FileContent;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.iceberg.PartitionTransforms;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.iceberg.hive.IcebergFileHiveMetastore;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.TableScanUtil;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.FileContent.EQUALITY_DELETES;
import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.IcebergSessionProperties.DELETE_AS_JOIN_REWRITE_ENABLED;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;

@Test
public class TestIcebergEqualityDeletesNative
        extends AbstractTestQueryFramework
{
    private String fileFormat;

    TestIcebergEqualityDeletesNative()
    {
        fileFormat = "PARQUET";
    }

    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils
                .nativeIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET").build();
    }

    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils
                .javaIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET").build();
    }

    protected HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HiveS3Config hiveS3Config = new HiveS3Config();
        return getHdfsEnvironment(hiveClientConfig, metastoreClientConfig, hiveS3Config);
    }

    public static HdfsEnvironment getHdfsEnvironment(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig, HiveS3Config hiveS3Config)
    {
        S3ConfigurationUpdater s3ConfigurationUpdater = new PrestoS3ConfigurationUpdater(hiveS3Config);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig, s3ConfigurationUpdater, ignored -> {}, ignored -> {}),
                ImmutableSet.of(), hiveClientConfig);
        return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
    }

    private Table updateTable(String tableName)
    {
        BaseTable table = (BaseTable) loadTable(tableName);
        TableOperations operations = table.operations();
        TableMetadata currentMetadata = operations.current();
        operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(2));

        return table;
    }

    protected Path getCatalogDirectory()
    {
        java.nio.file.Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        return new Path(getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false).toFile().toURI());
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        IcebergFileHiveMetastore fileHiveMetastore = new IcebergFileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().toString(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    protected Table loadTable(String tableName)
    {
        tableName = normalizeIdentifier(tableName, ICEBERG_CATALOG);
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();

        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024 * 1024),
                getQueryRunner().getDefaultSession().toConnectorSession(connectorId),
                new IcebergCatalogName(ICEBERG_CATALOG),
                SchemaTableName.valueOf("tpch." + tableName));
    }

    private void testCheckDeleteFiles(Table icebergTable, int expectedSize, List<FileContent> expectedFileContent)
    {
        // check delete file list
        TableScan tableScan = icebergTable.newScan().useSnapshot(icebergTable.currentSnapshot().snapshotId());
        Iterator<FileScanTask> iterator = TableScanUtil.splitFiles(tableScan.planFiles(), tableScan.targetSplitSize()).iterator();
        List<DeleteFile> deleteFiles = new ArrayList<>();
        while (iterator.hasNext()) {
            FileScanTask task = iterator.next();
            List<org.apache.iceberg.DeleteFile> deletes = task.deletes();
            deletes.forEach(delete -> deleteFiles.add(DeleteFile.fromIceberg(delete)));
        }

        assertEquals(deleteFiles.size(), expectedSize);
        List<FileContent> fileContents = deleteFiles.stream().map(DeleteFile::content).sorted().collect(Collectors.toList());
        assertEquals(fileContents, expectedFileContent);
    }

    private void writeEqualityDeleteToNationTable(Table icebergTable, Map<String, Object> overwriteValues)
            throws Exception
    {
        writeEqualityDeleteToNationTable(icebergTable, overwriteValues, Collections.emptyMap());
    }

    private void writeEqualityDeleteToNationTable(Table icebergTable, Map<String, Object> overwriteValues, Map<String, Object> partitionValues)
            throws Exception
    {
        java.nio.file.Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        File metastoreDir = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false).toFile();
        Path metadataDir = new Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + randomUUID();
        FileSystem fs = getHdfsEnvironment().getFileSystem(new HdfsContext(SESSION), metadataDir);
        Schema deleteRowSchema = icebergTable.schema().select(overwriteValues.keySet());
        Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(HadoopOutputFile.fromPath(new Path(metadataDir, deleteFileName), fs))
                .forTable(icebergTable)
                .rowSchema(deleteRowSchema)
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(deleteRowSchema.columns().stream().map(Types.NestedField::fieldId).collect(Collectors.toList()))
                .overwrite();

        if (!partitionValues.isEmpty()) {
            GenericRecord partitionData = GenericRecord.create(icebergTable.spec().partitionType());
            writerBuilder.withPartition(partitionData.copy(partitionValues));
        }

        EqualityDeleteWriter<Object> writer = writerBuilder.buildEqualityWriter();

        Record dataDelete = GenericRecord.create(deleteRowSchema);
        try (Closeable ignored = writer) {
            writer.write(dataDelete.copy(overwriteValues));
        }
        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    private void writePositionDeleteToNationTable(Table icebergTable, String dataFilePath, long deletePos)
            throws IOException
    {
        java.nio.file.Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        File metastoreDir = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false).toFile();
        Path metadataDir = new Path(metastoreDir.toURI());
        String deleteFileName = "delete_file_" + randomUUID();
        FileSystem fs = getHdfsEnvironment().getFileSystem(new HdfsContext(SESSION), metadataDir);
        Path path = new Path(metadataDir, deleteFileName);
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(HadoopOutputFile.fromPath(path, fs))
                .createWriterFunc(GenericParquetWriter::create)
                .forTable(icebergTable)
                .overwrite()
                .rowSchema(icebergTable.schema())
                .withSpec(PartitionSpec.unpartitioned())
                .buildPositionWriter();

        PositionDelete<Record> positionDelete = PositionDelete.create();
        PositionDelete<Record> record = positionDelete.set(dataFilePath, deletePos, GenericRecord.create(icebergTable.schema()));
        try (Closeable ignored = writer) {
            writer.write(record);
        }

        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    private Session deleteAsJoinDisabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, DELETE_AS_JOIN_REWRITE_ENABLED, "false")
                .build();
    }

    @Test
    public void testEqualityDeleteWithPartitionColumnMissingInSelect()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        try {
            assertUpdateExpected("CREATE TABLE " + tableName + "(a int, b varchar) WITH (\"write.format.default\" = '" + fileFormat + "', partitioning=ARRAY['a'])");
            assertUpdateExpected("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002'), (2, '1010'), (3, '1003')", 4);

            Table icebergTable = updateTable(tableName);
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 2, "b", "1002"), ImmutableMap.of("a", 2));
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("b", "1010"), ImmutableMap.of("a", 2));
            assertQuery(session, "SELECT b FROM " + tableName, "VALUES ('1001'), ('1003')");
            assertQuery(session, "SELECT b FROM " + tableName + " WHERE a > 1", "VALUES ('1003')");

            assertUpdateExpected("ALTER TABLE " + tableName + " ADD COLUMN c int WITH (partitioning = 'identity')");
            assertUpdateExpected("INSERT INTO " + tableName + " VALUES (6, '1004', 1), (6, '1006', 2), (6, '1009', 2)", 3);
            icebergTable = updateTable(tableName);
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 6, "c", 2, "b", "1006"),
                    ImmutableMap.of("a", 6, "c", 2));
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("b", "1009"),
                    ImmutableMap.of("a", 6, "c", 2));
            assertQuery(session, "SELECT a, b FROM " + tableName, "VALUES (1, '1001'), (3, '1003'), (6, '1004')");
            assertQuery(session, "SELECT a, b FROM " + tableName + " WHERE a in (1, 6) and c < 3", "VALUES (6, '1004')");

            assertUpdateExpected("ALTER TABLE " + tableName + " ADD COLUMN d varchar WITH (partitioning = 'truncate(2)')");
            assertUpdateExpected("INSERT INTO " + tableName + " VALUES (6, '1004', 1, 'th001'), (6, '1006', 2, 'th002'), (6, '1006', 3, 'ti003')", 3);
            icebergTable = updateTable(tableName);
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 6, "c", 1, "d", "th001"),
                    ImmutableMap.of("a", 6, "c", 1, "d_trunc", "th"));
            testCheckDeleteFiles(icebergTable, 5, ImmutableList.of(EQUALITY_DELETES, EQUALITY_DELETES, EQUALITY_DELETES, EQUALITY_DELETES, EQUALITY_DELETES));
            assertQuery(session, "SELECT a, b, d FROM " + tableName, "VALUES (1, '1001', NULL), (3, '1003', NULL), (6, '1004', NULL), (6, '1006', 'th002'), (6, '1006', 'ti003')");
            assertQuery(session, "SELECT a, b, d FROM " + tableName + " WHERE a in (1, 6) and d > 'th000'", "VALUES (6, '1006', 'th002'), (6, '1006', 'ti003')");
        }
        finally {
            assertUpdateExpected("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testTableWithEqualityDelete()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_equality_delete" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L));
        testCheckDeleteFiles(icebergTable, 1, ImmutableList.of(EQUALITY_DELETES));
        assertQuery(session, "SELECT * FROM " + tableName);
        assertQuery(session, "SELECT nationkey FROM " + tableName);
    }

    @Test
    public void testTableWithEqualityDeleteDifferentColumnOrder()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        // Specify equality delete filter with different column order from table definition
        String tableName = "test_v2_equality_delete_different_order" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L, "name", "ARGENTINA"));
        assertQuery(session, "SELECT * FROM " + tableName);
        // natiokey is before the equality delete column in the table schema, comment is after
        assertQuery(session, "SELECT nationkey, comment FROM " + tableName);
    }

    @Test
    public void testTableWithEqualityDeleteAndGroupByAndLimit()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        // Specify equality delete filter with different column order from table definition
        String tableName = "test_v2_equality_delete_different_order" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L, "name", "ARGENTINA"));
        assertQuery(session, "SELECT * FROM " + tableName);

        // Test group by
        assertQuery(session, "SELECT nationkey FROM " + tableName + " group by nationkey");
        // Test group by with limit
        assertQuery(session, "SELECT nationkey FROM " + tableName + " group by nationkey limit 100");
    }

    @Test
    public void testTableWithPositionDeleteAndEqualityDelete()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);
        String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1").getOnlyValue();

        writePositionDeleteToNationTable(icebergTable, dataFilePath, 0);
        testCheckDeleteFiles(icebergTable, 1, ImmutableList.of(POSITION_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 24");
        assertQuery(session, "SELECT nationkey FROM " + tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L));
        testCheckDeleteFiles(icebergTable, 2, ImmutableList.of(POSITION_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT * FROM " + tableName);
        assertQuery(session, "SELECT nationkey FROM " + tableName);
    }

    @Test
    public void testPartitionedTableWithEqualityDelete()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_equality_delete" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['nationkey'], \"write.format.default\" = '" + fileFormat + "') " + " AS SELECT * FROM tpch.tiny.nation", 25);
        Table icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L, "nationkey", 1L), ImmutableMap.of("nationkey", 1L));
        assertQuery(session, "SELECT * FROM " + tableName);
        assertQuery(session, "SELECT nationkey, comment FROM " + tableName);
    }

    @Test
    public void testEqualityDeletesWithPartitions()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "', partitioning = ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);

        List<Long> partitions = Arrays.asList(1L, 2L, 3L, 17L, 24L);
        for (long nationKey : partitions) {
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L), ImmutableMap.of("nationkey", nationKey));
        }
        testCheckDeleteFiles(icebergTable, partitions.size(), partitions.stream().map(i -> EQUALITY_DELETES).collect(Collectors.toList()));
        assertQuery(session, "SELECT * FROM " + tableName);
        assertQuery(session, "SELECT nationkey FROM " + tableName);
        assertQuery(session, "SELECT name FROM " + tableName);
    }

    @Test
    public void testEqualityDeletesWithHiddenPartitions()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "', partitioning = ARRAY['bucket(nationkey,100)']) AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);

        PartitionTransforms.ColumnTransform columnTransform = PartitionTransforms.getColumnTransform(icebergTable.spec().fields().get(0), BIGINT);
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(5);
        List<Long> partitions = Arrays.asList(1L, 2L, 3L, 17L, 24L);
        partitions.forEach(p -> BIGINT.writeLong(builder, p));
        Block partitionsBlock = columnTransform.getTransform().apply(builder.build());
        for (int i = 0; i < partitionsBlock.getPositionCount(); i++) {
            writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L), ImmutableMap.of("nationkey_bucket", partitionsBlock.getInt(i)));
            assertQuery(session, "SELECT * FROM " + tableName);
        }
        testCheckDeleteFiles(icebergTable, partitions.size(), partitions.stream().map(i -> EQUALITY_DELETES).collect(Collectors.toList()));
        assertQuery(session, "SELECT * FROM " + tableName);
        assertQuery(session, "SELECT nationkey FROM " + tableName);
        assertQuery(session, "SELECT name FROM " + tableName);
    }

    @Test
    public void testEqualityDeletesWithCompositeKey()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 0L, "name", "ALGERIA"));
        testCheckDeleteFiles(icebergTable, 1, Stream.generate(() -> EQUALITY_DELETES).limit(1).collect(Collectors.toList()));
        assertQuery(session, "SELECT * FROM " + tableName);
        assertQuery(session, "SELECT nationkey FROM " + tableName);
        assertQuery(session, "SELECT name FROM " + tableName);
    }

    @Test
    public void testEqualityDeletesWithMultipleDeleteSchemas()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L));
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("nationkey", 10L));
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 2L, "nationkey", 9L));
        testCheckDeleteFiles(icebergTable, 3, Stream.generate(() -> EQUALITY_DELETES).limit(3).collect(Collectors.toList()));
        // TODO: Support querying from metadata table equality_deletes in native mode.
        //assertQuery(session, "SELECT \"$data_sequence_number\", regionkey, nationkey FROM \"" + tableName + "$equality_deletes\"");
        assertQuery(session, "SELECT * FROM " + tableName);
        assertQuery(session, "SELECT regionkey FROM " + tableName);
        assertQuery(session, "SELECT nationkey FROM " + tableName);
    }

    public void testTableWithPositionDeletesAndEqualityDeletes()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + " with (\"write.format.default\" = '" + fileFormat + "') AS SELECT * FROM tpch.tiny.nation order by nationkey", 25);
        Table icebergTable = updateTable(tableName);
        String dataFilePath = (String) computeActual("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1").getOnlyValue();

        writePositionDeleteToNationTable(icebergTable, dataFilePath, 0);
        testCheckDeleteFiles(icebergTable, 1, ImmutableList.of(POSITION_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 24");
        assertQuery(session, "SELECT nationkey FROM " + tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 1L));
        testCheckDeleteFiles(icebergTable, 2, ImmutableList.of(POSITION_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 19");
        assertQuery(session, "SELECT nationkey FROM " + tableName);

        writePositionDeleteToNationTable(icebergTable, dataFilePath, 7);
        testCheckDeleteFiles(icebergTable, 3, ImmutableList.of(POSITION_DELETES, POSITION_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 18");
        assertQuery(session, "SELECT nationkey FROM " + tableName);

        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("regionkey", 2L));
        testCheckDeleteFiles(icebergTable, 4, ImmutableList.of(POSITION_DELETES, POSITION_DELETES, EQUALITY_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES 13");
        assertQuery(session, "SELECT nationkey FROM " + tableName);
    }

    @Test
    public void testEqualityDeletesWithHiddenPartitionsEvolution()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + "(a int, b varchar) WITH (\"write.format.default\" = '" + fileFormat + "')");
        assertUpdateExpected("INSERT INTO " + tableName + " VALUES (1, '1001'), (2, '1002'), (3, '1003')", 3);

        Table icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 2));
        assertQuery(session, "SELECT * FROM " + tableName);

        assertUpdateExpected("ALTER TABLE " + tableName + " ADD COLUMN c int WITH (partitioning = 'bucket(2)')");
        assertUpdateExpected("INSERT INTO " + tableName + " VALUES (6, '1004', 1), (6, '1006', 2)", 2);
        icebergTable = updateTable(tableName);
        PartitionTransforms.ColumnTransform columnTransform = PartitionTransforms.getColumnTransform(icebergTable.spec().fields().get(0), INTEGER);
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(1);
        List<Integer> partitions = Arrays.asList(1, 2);
        partitions.forEach(p -> BIGINT.writeLong(builder, p));
        Block partitionsBlock = columnTransform.getTransform().apply(builder.build());
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 6, "c", 2),
                ImmutableMap.of("c_bucket", partitionsBlock.getInt(0)));
        assertQuery(session, "SELECT * FROM " + tableName);

        assertUpdateExpected("ALTER TABLE " + tableName + " ADD COLUMN d varchar WITH (partitioning = 'truncate(2)')");
        assertUpdateExpected("INSERT INTO " + tableName + " VALUES (6, '1004', 1, 'th001'), (6, '1006', 2, 'th002')", 2);
        icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("a", 6, "c", 1),
                ImmutableMap.of("c_bucket", partitionsBlock.getInt(0), "d_trunc", "th"));
        testCheckDeleteFiles(icebergTable, 3, ImmutableList.of(EQUALITY_DELETES, EQUALITY_DELETES, EQUALITY_DELETES));
        assertQuery(session, "SELECT * FROM " + tableName);
    }

    @Test
    public void testEqualityDeletesWithDataSequenceNumber()
            throws Exception
    {
        Session session = deleteAsJoinDisabled();
        String tableName = "test_v2_row_delete_" + randomTableSuffix();
        String tableName2 = "test_v2_row_delete_2_" + randomTableSuffix();
        assertUpdateExpected("CREATE TABLE " + tableName + "(id int, data varchar) WITH (\"write.format.default\" = '" + fileFormat + "')");
        assertUpdateExpected("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        assertUpdateExpected("CREATE TABLE " + tableName2 + "(id int, data varchar) WITH (\"write.format.default\" = '" + fileFormat + "')");
        assertUpdateExpected("INSERT INTO " + tableName2 + " VALUES (1, 'a')", 1);

        Table icebergTable = updateTable(tableName);
        writeEqualityDeleteToNationTable(icebergTable, ImmutableMap.of("id", 1));

        Table icebergTable2 = updateTable(tableName2);
        writeEqualityDeleteToNationTable(icebergTable2, ImmutableMap.of("id", 1));

        assertUpdateExpected("INSERT INTO " + tableName + " VALUES (1, 'b'), (2, 'a'), (3, 'a')", 3);
        assertUpdateExpected("INSERT INTO " + tableName2 + " VALUES (1, 'b'), (2, 'a'), (3, 'a')", 3);

        assertQuery(session, "SELECT * FROM " + tableName);

        assertQuery(session, "SELECT \"$data_sequence_number\", * FROM " + tableName);

        assertQuery(session, "SELECT a.\"$data_sequence_number\", b.\"$data_sequence_number\" from " + tableName + " as a, " + tableName2 + " as b where a.id = b.id");
    }
}
