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

import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.iceberg.hive.IcebergFileHiveMetastore;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static org.apache.iceberg.SnapshotRef.MAIN_BRANCH;
import static org.testng.Assert.assertEquals;

/**
 * Regression tests for specific bugs the V3 deletion-vector PR (#28182) fixed,
 * complementing the broader {@link TestNativeIcebergV3DeletionVectors} suite.
 *
 * <p>Each test here reproduces a scenario that an earlier, broken version of the
 * connector would silently mishandle (double DVs on a data file, UPDATE paths
 * that bypass DV commit, a DELETE blocked by a stale table property, or a branch
 * mutation seeded from the main snapshot's DV). The broader suite covers the
 * happy path with stronger file-level assertions; these tests focus on the
 * specific invariants the PR body calls out.
 *
 * <p>All DML runs on the native (Prestissimo) worker, which is the only runner
 * that can write Puffin deletion vectors. Row results use explicit VALUES; live
 * delete files are counted through the Iceberg API ({@code planFiles().deletes()}),
 * since Presto's {@code $files} metadata table lists data files only and cannot
 * see DVs. The DV-counting plumbing mirrors {@link TestNativeIcebergV3DeletionVectors}.
 */
public class TestNativeIcebergV3DeletionVectorRegressions
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    /**
     * Regression for the partition-aware DV replacement bug the PR fixes. An
     * earlier {@code toIcebergDeletionVector} reconstructed the prior DV with
     * {@code PartitionSpec.unpartitioned()}; {@code ManifestFilterManager} prunes
     * candidate manifests via {@code ManifestFileUtil.canContainAny}, and an
     * empty partition tuple cannot match a partitioned manifest. The manifest
     * holding the prior DV was skipped, the DV silently survived, and the data
     * file ended up with two DVs. The failure only surfaces on partitioned V3
     * tables under repeated mutation of the same file -- unpartitioned tests
     * (and {@link TestNativeIcebergV3DeletionVectors#testDeleteThenDeleteSameDataFile})
     * cannot see it.
     */
    @Test
    public void testPartitionedTableDeletionVectorReplacement()
    {
        String tableName = "v3_dv_partitioned_replacement";
        try {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, part VARCHAR) WITH (\"format-version\" = '3', partitioning = ARRAY['part'])");
            // All five rows land in the same partition, so both DELETEs target the
            // same data file -- the replacement path, not the two-separate-files path.
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a')", 5);

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertQuery("SELECT id FROM " + tableName + " ORDER BY id", "VALUES 1, 3, 4, 5");
            assertEquals(puffinDeleteFileCount(tableName), 1L, "expected one DV after first DELETE on partition 'a'");

            // Second DELETE on the same data file. If the replacement path reconstructs
            // the prior DV with an unpartitioned spec, manifest pruning skips the manifest
            // that holds it and the prior DV is retained -- two DVs on one data file.
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 4", 1);
            assertQuery("SELECT id FROM " + tableName + " ORDER BY id", "VALUES 1, 3, 5");
            assertEquals(puffinDeleteFileCount(tableName), 1L,
                    "expected one merged DV after second DELETE on the same partitioned data file "
                            + "(a count > 1 indicates the partition-spec regression in toIcebergDeletionVector)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * V3 UPDATE lands as a Puffin DV marking the pre-image position on the base
     * data file plus a new data file holding the updated row.
     * {@link TestNativeIcebergV3DeletionVectors} exercises UPDATE semantics only
     * through MERGE's {@code WHEN MATCHED THEN UPDATE SET}; the planner path that
     * rewrites plain {@code UPDATE} to MERGE is a separate concern. This test
     * exercises the explicit {@code UPDATE} SQL and pins (a) one Puffin DV is
     * written on the base data file and (b) no Parquet position-delete file is
     * written (which V3 forbids).
     *
     * <p>DV replacement through UPDATE is intentionally not asserted here --
     * every UPDATE writes a fresh data file for the new row, so repeated updates
     * to one id put each DV on a different data file; the one-DV-per-file
     * invariant holds trivially. {@link TestNativeIcebergV3DeletionVectors#testMergeAfterPriorDelete}
     * covers the real DV-replacement path (prior DELETE's DV merged into a
     * subsequent MERGE on the same file).
     */
    @Test
    public void testUpdateWritesDeletionVector()
    {
        String tableName = "v3_dv_update";
        try {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, status VARCHAR, score DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'new', 10.0), (2, 'new', 20.0), (3, 'new', 30.0)", 3);

            // Explicit UPDATE SQL -- the planner's UPDATE-to-MERGE rewrite is
            // what wires this to the DV write sink.
            // TODO: Assert the row count (1) once native UPDATE stops counting
            // its own insert half.
            assertQuerySucceeds("UPDATE " + tableName + " SET status = 'done' WHERE id = 2");
            assertQuery("SELECT id, status, score FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'new', 10.0), (2, 'done', 20.0), (3, 'new', 30.0)");
            assertEquals(puffinDeleteFileCount(tableName), 1L, "expected one DV after UPDATE");
            assertEquals(parquetPositionDeleteFileCount(tableName), 0L, "V3 UPDATE must not write Parquet position deletes");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * V2 required {@code write.delete.mode = 'merge-on-read'} on the table before any
     * row-level DELETE was accepted. V3 drops that requirement -- the DV write path is
     * the only V3 option, so the property is implied. The PR's release notes call this
     * out explicitly; this test pins the behavior so the gate cannot regress silently.
     */
    @Test
    public void testDeleteDoesNotRequireMergeOnReadProperty()
    {
        String tableName = "v3_dv_default_delete_mode";
        try {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            // No write.delete.mode -- V3 must accept DELETE anyway.
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, v VARCHAR) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertQuery("SELECT id FROM " + tableName + " ORDER BY id", "VALUES 1, 3");
            assertEquals(puffinDeleteFileCount(tableName), 1L, "expected a DV even without write.delete.mode set");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * Regression for the branch-snapshot scoping of
     * {@code IcebergAbstractMetadata.enumerateExistingDeletionVectors}. A V3 branch
     * DELETE must resolve its read snapshot from the handle's branch id (branch head),
     * not {@code icebergTable.currentSnapshot()} (main head). If enumeration leaks to
     * main, the branch mutation seeds its new DV from main's prior DV on the shared
     * data file, and the branch loses positions that were only ever deleted on main.
     *
     * <p>{@link TestNativeIcebergV3DeletionVectors#testDeleteOnBranch} covers the
     * "branch has a DV, main has none" direction. This test produces the inverse
     * failure mode: main carries a prior DV, then the branch deletes a different
     * position; the branch's DV must mark only its own position, not inherit main's.
     */
    @Test
    public void testBranchDeleteDoesNotSeedFromMainDeletionVector()
    {
        String tableName = "v3_dv_branch_scoping";
        String branchName = "v3_branch";
        String branchRef = "\"" + tableName + ".branch_" + branchName + "\"";
        try {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, v VARCHAR) WITH (\"format-version\" = '3')");
            // Three rows in one data file; the branch and main will each carry a
            // DV on this file.
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
            assertUpdate("ALTER TABLE " + tableName + " CREATE BRANCH '" + branchName + "'");

            // Main's DV marks id=2.
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertEquals(puffinDeleteFileCount(tableName), 1L, "expected one DV on main after the main-side DELETE");

            // Branch DELETE on a different position. If enumeration reads main's
            // snapshot, this commits a DV marking BOTH id=2 (seeded from main) and
            // id=1 (requested) -- the branch would then lose id=2, which main
            // deleted but the branch never did.
            assertUpdate("DELETE FROM " + branchRef + " WHERE id = 1", 1);

            // Branch keeps id=2 (never deleted on branch) and id=3; loses only id=1.
            assertQuery("SELECT id FROM " + branchRef + " ORDER BY id", "VALUES 2, 3");

            // Main keeps id=1 (never deleted on main) and id=3; loses only id=2.
            assertQuery("SELECT id FROM " + tableName + " ORDER BY id", "VALUES 1, 3");

            // Each ref carries exactly its own DV on the shared data file.
            assertEquals(puffinDeleteFileCount(tableName), 1L, "main still has one DV, untouched by the branch DELETE");
            assertEquals(puffinDeleteFileCount(tableName, branchName), 1L, "branch has its own DV, not shared with main");

            assertUpdate("ALTER TABLE " + tableName + " DROP BRANCH '" + branchName + "'");

            // Dropping the branch must not disturb main's own DV: the branch-only
            // DV is unreferenced after the drop, but main's DV lives on main's
            // snapshot and is unaffected.
            assertEquals(puffinDeleteFileCount(tableName), 1L, "main's DV survives dropping the branch");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // -- Delete-file counting helpers --------------------------------------------
    //
    // Presto's $files metadata table lists data files only (FilesTable iterates
    // fileScanTask.file(), never task.deletes()), so Puffin DVs are invisible to
    // it: content = 1 AND file_format = 'PUFFIN' against $files always returns 0
    // even when the commit wrote DVs. Count live delete files through the Iceberg
    // API instead: planFiles() surfaces each FileScanTask's deletes(), and a V3
    // DV is a POSITION_DELETES entry stored in a PUFFIN file. Mirrors
    // TestNativeIcebergV3DeletionVectors; kept local rather than hoisted to avoid
    // churn in that file.
    private long puffinDeleteFileCount(String table)
    {
        return deleteFileCount(table, MAIN_BRANCH, true);
    }

    private long puffinDeleteFileCount(String table, String ref)
    {
        return deleteFileCount(table, ref, true);
    }

    private long parquetPositionDeleteFileCount(String table)
    {
        return deleteFileCount(table, MAIN_BRANCH, false);
    }

    private long deleteFileCount(String table, String ref, boolean puffin)
    {
        Table icebergTable = loadIcebergTable(table);
        long count = 0;
        try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().useRef(ref).planFiles()) {
            for (FileScanTask task : tasks) {
                for (org.apache.iceberg.DeleteFile deleteFile : task.deletes()) {
                    boolean isPuffin = deleteFile.format() == org.apache.iceberg.FileFormat.PUFFIN;
                    if (isPuffin == puffin) {
                        count++;
                    }
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return count;
    }

    private Table loadIcebergTable(String tableName)
    {
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();
        ConnectorSession connectorSession = getSession().toConnectorSession(connectorId);

        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig(), new HiveS3Config());
        ExtendedHiveMetastore metastore = memoizeMetastore(
                new IcebergFileHiveMetastore(hdfsEnvironment, getCatalogDirectory().toString(), "test"),
                false,
                1000,
                0);

        return IcebergUtil.getHiveIcebergTable(
                metastore,
                hdfsEnvironment,
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024),
                connectorSession,
                new IcebergCatalogName(ICEBERG_CATALOG),
                SchemaTableName.valueOf("tpch." + normalizeIdentifier(tableName, ICEBERG_CATALOG)));
    }

    private static HdfsEnvironment getHdfsEnvironment(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig, HiveS3Config hiveS3Config)
    {
        S3ConfigurationUpdater s3ConfigurationUpdater = new PrestoS3ConfigurationUpdater(hiveS3Config);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig, s3ConfigurationUpdater, ignored -> {}, ignored -> {}),
                ImmutableSet.of(),
                hiveClientConfig);
        return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
    }

    private File getCatalogDirectory()
    {
        java.nio.file.Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        return getIcebergDataDirectoryPath(
                dataDirectory,
                HIVE.name(),
                new IcebergConfig().getFileFormat(),
                false)
                .toFile();
    }
}
