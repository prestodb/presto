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
package com.facebook.presto.iceberg;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.replaceDeletionVectors;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Commit-level coverage for the Iceberg V3 deletion-vector write path's core correctness
 * property: after repeated row-level mutations of the same data file, that data file must carry
 * exactly one deletion vector (Iceberg's one-DV-per-data-file invariant).
 *
 * <p>The full DELETE/UPDATE/MERGE round trip cannot run in a pure-Java query runner: the native
 * (Prestissimo) worker is what writes the Puffin DV blob and reads it back (the Java page source
 * rejects PUFFIN deletion vectors, see {@code TestIcebergV3.testPuffinDeletionVectorsNotSupported}),
 * so end-to-end SQL read-back is exercised by the native golden-query suite instead. What this test
 * does cover, entirely in Java, is the connector's commit logic that maintains the invariant:
 * {@link IcebergAbstractMetadata#replaceDeletionVectors} + the unpartitioned-spec reconstruction in
 * {@code IcebergAbstractMetadata.toIcebergDeletionVector}. It drives that logic against a real
 * Iceberg table (unpartitioned and partitioned) using hand-built Puffin DV entries — the same
 * technique {@code TestIcebergV3.testPuffinDeletionVectorsNotSupported} uses — and asserts no
 * duplicate DVs accumulate. In particular the partitioned case guards the documented assumption
 * that DV removal matches on {@code (location, contentOffset, contentSizeInBytes)} regardless of
 * partitioning; if that ever breaks, the prior DV would not be removed and the invariant assertion
 * would fail rather than silently leaving duplicate DVs.
 */
public class TestIcebergV3DeletionVectorRoundTrip
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";
    private static final int MUTATION_ROUNDS = 3;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    @Test
    public void testRepeatedDeletionVectorReplacementUnpartitioned()
    {
        String tableName = "test_v3_dv_roundtrip_unpartitioned";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, value VARCHAR) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);

            Table table = loadTable(tableName);
            DataFileInfo dataFile = singleDataFile(table);

            // Simulate the connector re-mutating the same data file repeatedly: each round the
            // worker writes a fresh merged DV, and the commit must drop the prior one so the data
            // file ends up with exactly one DV.
            for (int round = 0; round < MUTATION_ROUNDS; round++) {
                commitReplacementDeletionVector(table, dataFile, round);
                Multiset<String> dvs = puffinDeletionVectorsByDataFile(table);
                assertEquals(dvs.count(dataFile.path), 1,
                        "data file must carry exactly one deletion vector after round " + round);
                assertEquals(dvs.elementSet().size(), 1, "no other data file should gain a DV");
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRepeatedDeletionVectorReplacementPartitioned()
    {
        String tableName = "test_v3_dv_roundtrip_partitioned";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, value VARCHAR, part VARCHAR)"
                    + " WITH (\"format-version\" = '3', partitioning = ARRAY['part'])");
            // A single partition => a single data file to re-mutate.
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'one', 'A'), (2, 'two', 'A'), (3, 'three', 'A')", 3);

            Table table = loadTable(tableName);
            DataFileInfo dataFile = singleDataFile(table);

            for (int round = 0; round < MUTATION_ROUNDS; round++) {
                commitReplacementDeletionVector(table, dataFile, round);
                Multiset<String> dvs = puffinDeletionVectorsByDataFile(table);
                // The removal reconstructs the prior DV with PartitionSpec.unpartitioned(); this
                // asserts that still matches the partitioned manifest entry, so exactly one DV
                // remains rather than a duplicate per re-mutation.
                assertEquals(dvs.count(dataFile.path), 1,
                        "partitioned data file must carry exactly one deletion vector after round " + round);
                assertEquals(dvs.elementSet().size(), 1, "no other data file should gain a DV");
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * Mirrors the connector's V3 delete commit for a single data file: add a freshly written Puffin
     * DV and remove the prior one via the production {@link IcebergAbstractMetadata#replaceDeletionVectors}
     * helper, bounding the conflict check to the read snapshot.
     */
    private void commitReplacementDeletionVector(Table table, DataFileInfo dataFile, int round)
    {
        long readSnapshotId = table.currentSnapshot().snapshotId();
        Map<String, com.facebook.presto.iceberg.delete.DeleteFile> existingDeletionVectors =
                enumerateExistingDeletionVectors(table);

        // Each round writes a distinct Puffin blob (distinct path/offset), so the new DV and the
        // prior DV being removed have distinct identities.
        FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(dataFile.spec)
                .ofPositionDeletes()
                .withPath(dataFile.path + ".dv" + round + ".puffin")
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(16L * (round + 1))
                .withRecordCount(round + 1L)
                .withReferencedDataFile(dataFile.path)
                .withContentOffset(round * 100L)
                .withContentSizeInBytes(16L);
        if (dataFile.spec.isPartitioned()) {
            builder.withPartition(dataFile.partition);
        }

        RowDelta rowDelta = table.newRowDelta();
        org.apache.iceberg.DeleteFile addedDeletionVector = builder.build();
        rowDelta.addDeletes(addedDeletionVector);
        replaceDeletionVectors(
                rowDelta,
                Optional.of(readSnapshotId),
                existingDeletionVectors,
                ImmutableMap.of(dataFile.path, addedDeletionVector),
                table.specs());
        rowDelta.commit();
        table.refresh();
    }

    /**
     * Enumerates the current snapshot's Puffin deletion vectors keyed by referenced data file,
     * mirroring {@code IcebergAbstractMetadata.enumerateExistingDeletionVectors} (which is private).
     */
    private static Map<String, com.facebook.presto.iceberg.delete.DeleteFile> enumerateExistingDeletionVectors(Table table)
    {
        Snapshot snapshot = table.currentSnapshot();
        Map<String, com.facebook.presto.iceberg.delete.DeleteFile> deletionVectors = new HashMap<>();
        if (snapshot == null) {
            return deletionVectors;
        }
        Map<Integer, PartitionSpec> specsById = table.specs();
        for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
            try (CloseableIterable<org.apache.iceberg.DeleteFile> deleteFiles =
                    ManifestFiles.readDeleteManifest(manifest, table.io(), specsById)) {
                for (org.apache.iceberg.DeleteFile deleteFile : deleteFiles) {
                    if (deleteFile.format() == FileFormat.PUFFIN && deleteFile.referencedDataFile() != null) {
                        deletionVectors.putIfAbsent(
                                deleteFile.referencedDataFile().toString(),
                                com.facebook.presto.iceberg.delete.DeleteFile.fromIceberg(deleteFile));
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to read delete manifest: " + manifest.path(), e);
            }
        }
        return deletionVectors;
    }

    private static Multiset<String> puffinDeletionVectorsByDataFile(Table table)
    {
        ImmutableMultiset.Builder<String> referencedDataFiles = ImmutableMultiset.builder();
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return referencedDataFiles.build();
        }
        Map<Integer, PartitionSpec> specsById = table.specs();
        for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
            try (CloseableIterable<org.apache.iceberg.DeleteFile> deleteFiles =
                    ManifestFiles.readDeleteManifest(manifest, table.io(), specsById)) {
                for (org.apache.iceberg.DeleteFile deleteFile : deleteFiles) {
                    if (deleteFile.format() == FileFormat.PUFFIN && deleteFile.referencedDataFile() != null) {
                        referencedDataFiles.add(deleteFile.referencedDataFile().toString());
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to read delete manifest: " + manifest.path(), e);
            }
        }
        return referencedDataFiles.build();
    }

    private static DataFileInfo singleDataFile(Table table)
    {
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            FileScanTask task = tasks.iterator().next();
            assertNotNull(task, "expected at least one data file");
            return new DataFileInfo(task.file().path().toString(), task.file().partition(), task.spec());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to plan files", e);
        }
    }

    private Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + tableName);
    }

    private Map<String, String> getProperties()
    {
        return ImmutableMap.of("warehouse", getCatalogDirectory().toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(
                dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }

    private static final class DataFileInfo
    {
        private final String path;
        private final StructLike partition;
        private final PartitionSpec spec;

        private DataFileInfo(String path, StructLike partition, PartitionSpec spec)
        {
            this.path = path;
            this.partition = partition;
            this.spec = spec;
        }
    }
}
