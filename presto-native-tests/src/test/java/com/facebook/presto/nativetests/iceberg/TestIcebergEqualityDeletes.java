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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.iceberg.hive.IcebergFileHiveMetastore;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.MAX_DRIVERS_PER_TASK;
import static com.facebook.presto.SystemSessionProperties.NATIVE_MAX_SPLIT_PRELOAD_PER_DRIVER;
import static com.facebook.presto.SystemSessionProperties.SINGLE_NODE_EXECUTION_ENABLED;
import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.DELETE_AS_JOIN_REWRITE_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

/**
 * Tests native reads of Iceberg equality delete files, which Presto itself never writes, so the
 * delete files are committed here through the Iceberg API.
 */
public class TestIcebergEqualityDeletes
        extends AbstractTestQueryFramework
{
    // Partitions, and so splits, read by testEqualityDeleteOnColumnAddedByLaterSplit.
    private static final int PARTITION_COUNT = 4;
    // The catalog is loaded directly below, so pin the type the query runners are built with.
    private static final CatalogType CATALOG_TYPE = CatalogType.HIVE;

    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "false"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CATALOG_TYPE)
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CATALOG_TYPE)
                .setAddStorageFormatToPath(true)
                .build();
    }

    /**
     * Covers equality deletes on columns that the query does not project, where the splits of a single
     * driver need different delete columns. Single node execution, one driver per task and no split
     * preload leave every split sharing one scan spec, so each delete column is added to that spec only
     * once its own split is read, in whichever order the splits arrive.
     */
    @Test
    public void testEqualityDeleteOnColumnAddedByLaterSplit()
            throws Exception
    {
        QueryRunner javaQueryRunner = (QueryRunner) getExpectedQueryRunner();
        String tableName = "test_equality_delete_later_split";
        javaQueryRunner.execute("DROP TABLE IF EXISTS " + tableName);
        try {
            javaQueryRunner.execute("CREATE TABLE " + tableName +
                    "(first_key int, second_key int, part int, value varchar) WITH (partitioning = ARRAY['part'])");
            // One split per partition, each holding a row that stays and a row a delete removes.
            ImmutableList.Builder<String> insertedRows = ImmutableList.builder();
            ImmutableList.Builder<String> expectedRows = ImmutableList.builder();
            for (int part = 1; part <= PARTITION_COUNT; part++) {
                insertedRows.add(format("(%s, %s, %s, 'keep_%s')", keptKey(part), keptKey(part), part, part));
                insertedRows.add(format("(%s, %s, %s, 'drop_%s')", deletedKey(part), deletedKey(part), part, part));
                expectedRows.add(format("(%s, 'keep_%s')", part, part));
            }
            javaQueryRunner.execute("INSERT INTO " + tableName + " VALUES " + String.join(", ", insertedRows.build()));

            Table icebergTable = loadIcebergTable(tableName);
            for (int part = 1; part <= PARTITION_COUNT; part++) {
                writeEqualityDelete(
                        icebergTable,
                        ImmutableMap.of(equalityColumn(part), deletedKey(part)),
                        ImmutableMap.of("part", part));
            }

            // Without the delete-as-join rewrite the deletes are applied by the reader, not the plan.
            Session session = Session.builder(getSession())
                    .setSystemProperty(SINGLE_NODE_EXECUTION_ENABLED, "true")
                    .setSystemProperty(MAX_DRIVERS_PER_TASK, "1")
                    // Already the default, pinned because the shared scan spec depends on it.
                    .setSystemProperty(NATIVE_MAX_SPLIT_PRELOAD_PER_DRIVER, "0")
                    .setCatalogSessionProperty(ICEBERG_CATALOG, DELETE_AS_JOIN_REWRITE_ENABLED, "false")
                    .build();
            assertQuery(session, "SELECT part, value FROM " + tableName, "VALUES " + String.join(", ", expectedRows.build()));
            assertQuery(session, "SELECT count(*) FROM " + tableName, "VALUES " + PARTITION_COUNT);
        }
        finally {
            javaQueryRunner.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private static String equalityColumn(int part)
    {
        return part % 2 == 0 ? "first_key" : "second_key";
    }

    private static int keptKey(int part)
    {
        return 10 * part + 1;
    }

    private static int deletedKey(int part)
    {
        return 10 * part + 2;
    }

    private Table loadIcebergTable(String tableName)
    {
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment();
        ExtendedHiveMetastore metastore = memoizeMetastore(
                new IcebergFileHiveMetastore(hdfsEnvironment, getCatalogDirectory().toFile().toURI().toString(), "test"),
                false,
                1000,
                0);
        return IcebergUtil.getHiveIcebergTable(
                metastore,
                hdfsEnvironment,
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024 * 1024),
                getConnectorSession(),
                new IcebergCatalogName(ICEBERG_CATALOG),
                SchemaTableName.valueOf(getSession().getSchema().get() + "." + tableName));
    }

    /**
     * Commits an equality delete file, which Presto itself never writes, scoped to one partition.
     */
    private void writeEqualityDelete(Table icebergTable, Map<String, Object> deleteValues, Map<String, Object> partitionValues)
            throws Exception
    {
        Path deleteDirectory = new Path(getCatalogDirectory().toUri());
        FileSystem fileSystem = getHdfsEnvironment().getFileSystem(new HdfsContext(getConnectorSession()), deleteDirectory);
        Schema deleteRowSchema = icebergTable.schema().select(deleteValues.keySet());
        Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(
                        HadoopOutputFile.fromPath(new Path(deleteDirectory, "delete_file_" + randomUUID()), fileSystem))
                .forTable(icebergTable)
                .rowSchema(deleteRowSchema)
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(deleteRowSchema.columns().stream().map(Types.NestedField::fieldId).collect(Collectors.toList()))
                .withPartition(GenericRecord.create(icebergTable.spec().partitionType()).copy(partitionValues))
                .overwrite();

        EqualityDeleteWriter<Object> writer = writerBuilder.buildEqualityWriter();
        Record deleteRecord = GenericRecord.create(deleteRowSchema);
        try (Closeable ignored = writer) {
            writer.write(deleteRecord.copy(deleteValues));
        }
        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }

    private ConnectorSession getConnectorSession()
    {
        ConnectorId connectorId = getDistributedQueryRunner().getCoordinator().getCatalogManager()
                .getCatalog(ICEBERG_CATALOG).get().getConnectorId();
        return getSession().toConnectorSession(connectorId);
    }

    private java.nio.file.Path getCatalogDirectory()
    {
        return IcebergQueryRunner.getIcebergDataDirectoryPath(
                getDistributedQueryRunner().getCoordinator().getDataDirectory(),
                CATALOG_TYPE.name(),
                FileFormat.valueOf(ICEBERG_DEFAULT_STORAGE_FORMAT),
                true);
    }

    private static HdfsEnvironment getHdfsEnvironment()
    {
        return IcebergDistributedTestBase.getHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig(), new HiveS3Config());
    }
}
