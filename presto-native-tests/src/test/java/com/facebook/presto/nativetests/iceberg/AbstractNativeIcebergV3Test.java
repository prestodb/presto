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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.iceberg.hive.IcebergFileHiveMetastore;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

/**
 * Base for the Iceberg V3 end-to-end suites on native workers. The query runner uses native
 * workers; the expected runner uses Java workers over the same Iceberg warehouse, so
 * {@code assertQuery} checks native results against Java execution on the same data.
 *
 * <p>Every table is created with an explicit format version: the connector default is V2.
 * Native row-level DELETE, UPDATE and MERGE are still being implemented, so the suites that need
 * reliable row-level changes apply them with Java workers on V2 merge-on-read tables, which write
 * position-delete files.
 */
public abstract class AbstractNativeIcebergV3Test
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "iceberg_v3";
    private static final Pattern NATIVE_ROW_LEVEL_DML_FAILURE = Pattern.compile(
            "Field not found: _pos|Deletion-vector commit task for pre-V3 table|Content offset is required for DV|"
                    + "Iceberg table updates for format version 3 are not supported|type_error\\.304");

    private final String storageFormat;

    protected AbstractNativeIcebergV3Test(String storageFormat)
    {
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setSchemaName(SCHEMA)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setSchemaName(SCHEMA)
                .build();
    }

    @Override
    protected void createTables()
    {
        javaRunner().execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
    }

    /**
     * Writes tpch.tiny tables with Java workers, then copies them into V3 tables with native
     * workers, so the copies are native-written V3 data files.
     */
    protected void createTpchTables(String... tables)
    {
        for (String table : tables) {
            String seed = "seed_" + table;
            javaRunner().execute("DROP TABLE IF EXISTS " + seed);
            javaRunner().execute(format("CREATE TABLE %s WITH (format = '%s') AS SELECT * FROM tpch.tiny.%s", seed, storageFormat, table));
            createV3TableAs(table, "SELECT * FROM " + seed, scalarLong("SELECT count(*) FROM " + seed));
        }
    }

    protected QueryRunner javaRunner()
    {
        return (QueryRunner) getExpectedQueryRunner();
    }

    // Runs a DML statement on Java workers and checks its row count.
    protected void assertJavaUpdate(String sql, long expectedRows)
    {
        assertEquals(javaRunner().execute(sql).getUpdateCount().getAsLong(), expectedRows, sql);
    }

    // Native row-level DML may commit or fail while support is in progress. A commit must report the
    // expected row count and leave the expected rows; a failure must leave the table unchanged.
    // Only the known native row-level DML failures count as failing; any other error is rethrown.
    protected void assertNativeRowLevelDml(String sql, long expectedRows, String table, String expectedIfCommitted, String expectedIfFailed)
    {
        MaterializedResult result;
        try {
            result = getQueryRunner().execute(getSession(), sql);
        }
        catch (RuntimeException e) {
            if (e.getMessage() == null || !NATIVE_ROW_LEVEL_DML_FAILURE.matcher(e.getMessage()).find()) {
                throw e;
            }
            assertQuery("SELECT * FROM " + table, expectedIfFailed);
            return;
        }
        assertEquals(result.getUpdateCount().getAsLong(), expectedRows, sql);
        assertQuery("SELECT * FROM " + table, expectedIfCommitted);
    }

    protected String storageFormat()
    {
        return storageFormat;
    }

    protected String v3With()
    {
        return format("WITH (\"format-version\" = '3', format = '%s')", storageFormat);
    }

    protected String v3With(String extraProperties)
    {
        return format("WITH (\"format-version\" = '3', format = '%s', %s)", storageFormat, extraProperties);
    }

    protected void createV3Table(String table, String columns)
    {
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s (%s) %s", table, columns, v3With()));
    }

    protected void createV3Table(String table, String columns, String extraProperties)
    {
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s (%s) %s", table, columns, v3With(extraProperties)));
    }

    protected void createV3TableAs(String table, String query, long rows)
    {
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s %s AS %s", table, v3With(), query), rows);
    }

    protected void createV3TableAs(String table, String extraProperties, String query, long rows)
    {
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s %s AS %s", table, v3With(extraProperties), query), rows);
    }

    // Row-level DML needs a V2 table in merge-on-read mode.
    protected String v2MergeOnReadWith(String extraProperties)
    {
        return format("WITH (\"format-version\" = '2', format = '%s', \"write.delete.mode\" = 'merge-on-read', \"write.update.mode\" = 'merge-on-read'%s)",
                storageFormat, extraProperties.isEmpty() ? "" : ", " + extraProperties);
    }

    protected void createV2MergeOnReadTable(String table, String columns)
    {
        createV2MergeOnReadTable(table, columns, "");
    }

    protected void createV2MergeOnReadTable(String table, String columns, String extraProperties)
    {
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s (%s) %s", table, columns, v2MergeOnReadWith(extraProperties)));
    }

    protected void createV2MergeOnReadTableAs(String table, String query, long rows)
    {
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s %s AS %s", table, v2MergeOnReadWith(""), query), rows);
    }

    // sequence() is capped at 10,000 entries; build larger ranges from a cross join.
    protected static String rangeOneTo(long count)
    {
        if (count <= 10_000) {
            return format("SELECT x FROM UNNEST(sequence(1, %s)) AS t(x)", count);
        }
        checkArgument(count % 1000 == 0, "count must be a multiple of 1000");
        return format("SELECT CAST(a * 1000 + b AS bigint) AS x FROM UNNEST(sequence(0, %s)) AS t(a) CROSS JOIN UNNEST(sequence(1, 1000)) AS u(b)", count / 1000 - 1);
    }

    protected void dropTable(String table)
    {
        assertUpdate("DROP TABLE IF EXISTS " + table);
    }

    protected long scalarLong(String sql)
    {
        return ((Number) computeScalar(sql)).longValue();
    }

    // $files lists the data files of the current snapshot only; delete files are read through the Iceberg API.
    protected long dataFileCount(String table)
    {
        return scalarLong(format("SELECT count(*) FROM \"%s$files\"", table));
    }

    protected long snapshotCount(String table)
    {
        return scalarLong(format("SELECT count(*) FROM \"%s$snapshots\"", table));
    }

    protected long positionDeleteFileCount(String table)
    {
        return liveDeleteFiles(table).stream().filter(delete -> delete.content() == FileContent.POSITION_DELETES).count();
    }

    protected long positionDeleteRowCount(String table)
    {
        return liveDeleteFiles(table).stream().filter(delete -> delete.content() == FileContent.POSITION_DELETES).mapToLong(DeleteFile::recordCount).sum();
    }

    protected void assertDataFilesUseFormat(String table)
    {
        String extension = storageFormat.toLowerCase(ENGLISH);
        assertEquals(scalarLong(format("SELECT count(*) FROM \"%s$files\" WHERE file_path NOT LIKE '%%.%s'", table, extension)), 0L,
                format("expected every data file of %s to be a .%s file", table, extension));
    }

    protected List<FileScanTask> fileScanTasks(String table)
    {
        try (CloseableIterable<FileScanTask> tasks = loadTable(table).newScan().planFiles()) {
            return ImmutableList.copyOf(tasks);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Delete files of the current snapshot, de-duplicated across the data files they apply to.
    private List<DeleteFile> liveDeleteFiles(String table)
    {
        Map<String, DeleteFile> deletes = new LinkedHashMap<>();
        for (FileScanTask task : fileScanTasks(table)) {
            for (DeleteFile delete : task.deletes()) {
                deletes.putIfAbsent(delete.location(), delete);
            }
        }
        return ImmutableList.copyOf(deletes.values());
    }

    protected int formatVersion(String table)
    {
        return ((BaseTable) loadTable(table)).operations().current().formatVersion();
    }

    protected long currentSnapshotId(String table)
    {
        return loadTable(table).currentSnapshot().snapshotId();
    }

    protected String currentSnapshotOperation(String table)
    {
        return loadTable(table).currentSnapshot().operation();
    }

    // The runners use the HIVE catalog backed by a file metastore.
    protected Table loadTable(String table)
    {
        ConnectorId connectorId = getDistributedQueryRunner().getCoordinator().getCatalogManager().getCatalog(ICEBERG_CATALOG).get().getConnectorId();
        HdfsEnvironment hdfsEnvironment = IcebergDistributedTestBase.getHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig(), new HiveS3Config());
        ExtendedHiveMetastore metastore = memoizeMetastore(
                new IcebergFileHiveMetastore(hdfsEnvironment, catalogDirectory().toString(), "test"),
                false,
                1000,
                0);
        return IcebergUtil.getHiveIcebergTable(
                metastore,
                hdfsEnvironment,
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024),
                getSession().toConnectorSession(connectorId),
                new IcebergCatalogName(ICEBERG_CATALOG),
                new SchemaTableName(SCHEMA, table));
    }

    private Path catalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        return getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), FileFormat.valueOf(storageFormat), true);
    }
}
