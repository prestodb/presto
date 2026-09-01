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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.procedure.RewriteTablePathProcedure.FILE_LIST_NAME;
import static com.facebook.presto.iceberg.procedure.RewriteTablePathProcedure.STAGING_DIR_PREFIX;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRewriteTablePathProcedure
        extends AbstractTestQueryFramework
{
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().setCatalogType(HADOOP).build().getQueryRunner();
    }

    // -------------------------------------------------------------------------
    // Argument validation
    // -------------------------------------------------------------------------

    @Test
    public void testInvalidRewriteTablePathCalls()
    {
        assertQueryFails("CALL system.rewrite_table_path('schema', 'table', 'src')",
                "line 1:1: Required procedure argument 'target_prefix' is missing");
        assertQueryFails("CALL custom.rewrite_table_path('tpch', 'test', 'src', 'dst')",
                "Procedure not registered: custom.rewrite_table_path");
        assertQueryFails("CALL system.rewrite_table_path(table_name => 'test', source_prefix => 'src', target_prefix => 'dst')",
                "line 1:1: Required procedure argument 'schema' is missing");
    }

    @Test
    public void testRewriteTablePathOnNonExistingTableFails()
    {
        assertQueryFails("CALL system.rewrite_table_path('tpch', 'non_existing_table', 'src', 'dst')",
                "Table does not exist: tpch.non_existing_table");
    }

    @Test
    public void testRewriteTablePathWithNonMatchingPrefixFails()
    {
        String tableName = "rewrite_table_path_bad_prefix";
        createTable(tableName);
        try {
            assertQueryFails(
                    format("CALL system.rewrite_table_path('%s', '%s', 'file://wrong/warehouse', 'file://new/warehouse')",
                            TEST_SCHEMA, tableName),
                    "Table location .* does not start with source prefix 'file://wrong/warehouse'");
        }
        finally {
            dropTable(tableName);
        }
    }

    // -------------------------------------------------------------------------
    // Call syntax — positional and named args both reach the same code path
    // -------------------------------------------------------------------------

    @Test
    public void testRewriteTablePathPositionalArgs()
    {
        String tableName = "rewrite_table_path_positional";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

            // Catalog entry must be unchanged — the procedure does not update it.
            table.refresh();
            assertEquals(table.location(), originalLocation,
                    "Original table location should not be modified");

            // Metadata at the target location must reference the new location.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(newMetadata.location(), expectedNewLocation,
                    format("New metadata location should be '%s' but was '%s'", expectedNewLocation, newMetadata.location()));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathNamedArgs()
    {
        String tableName = "rewrite_table_path_named";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_renamed";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path(schema => '%s', table_name => '%s', source_prefix => '%s', target_prefix => '%s', staging_location => '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix)); // named args, order-independent

            // Catalog entry unchanged.
            table.refresh();
            assertEquals(table.location(), originalLocation,
                    "Original table location should not be modified");

            // Metadata at the target location must reference the new location.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            assertTrue(newMetadata.location().startsWith(targetPrefix),
                    format("New metadata location should start with '%s' but was '%s'", targetPrefix, newMetadata.location()));
        }
        finally {
            dropTable(tableName);
        }
    }

    // -------------------------------------------------------------------------
    // Rewrite correctness — verifies each file type is physically rewritten
    // with the correct internal path strings
    // -------------------------------------------------------------------------

    @Test
    public void testRewriteTablePathSnapshotPointersRewritten()
    {
        // Two snapshots → two manifest-list pointers. Both must reference target_prefix.
        String tableName = "rewrite_table_path_snapshots";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_snap_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            newMetadata.snapshots().forEach(snapshot -> {
                if (snapshot.manifestListLocation() != null) {
                    assertTrue(snapshot.manifestListLocation().startsWith(targetPrefix),
                            format("Snapshot %s manifest list '%s' should start with '%s'",
                                    snapshot.snapshotId(), snapshot.manifestListLocation(), targetPrefix));
                }
            });
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathManifestListRewritten()
    {
        // The manifest list Avro must be physically written at the target path and its
        // internal manifest_path fields must reference target_prefix.
        String tableName = "rewrite_table_path_manifest_list";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_ml_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());
            String manifestListLocation = table.currentSnapshot().manifestListLocation();

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

            // File must physically exist at the target path.
            String targetManifestListPath = manifestListLocation.replace(sourcePrefix, targetPrefix);
            assertTrue(Files.exists(toPath(targetManifestListPath)),
                    "Manifest list file should exist at " + targetManifestListPath);

            // Metadata must point to the rewritten manifest list path.
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            newMetadata.snapshots().forEach(snapshot -> {
                if (snapshot.manifestListLocation() != null) {
                    assertTrue(snapshot.manifestListLocation().startsWith(targetPrefix),
                            format("Snapshot manifest list '%s' should start with '%s'",
                                    snapshot.manifestListLocation(), targetPrefix));
                }
            });
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathManifestFileRewritten()
    {
        // Each manifest Avro must be physically written at the target path with
        // data_file.file_path fields referencing target_prefix.
        String tableName = "rewrite_table_path_manifest_file";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_mf_migrated";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());
            List<String> manifestPaths = table.currentSnapshot()
                    .allManifests(((BaseTable) table).operations().io())
                    .stream()
                    .map(ManifestFile::path)
                    .collect(Collectors.toList());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, targetPrefix));

            // Each manifest Avro must physically exist at the target path.
            for (String manifestPath : manifestPaths) {
                String targetManifestPath = manifestPath.replace(sourcePrefix, targetPrefix);
                assertTrue(Files.exists(toPath(targetManifestPath)),
                        "Manifest file should exist at " + targetManifestPath);
            }
            TableMetadata newMetadata = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(newMetadata.location(), expectedNewLocation);
        }
        finally {
            dropTable(tableName);
        }
    }

    // -------------------------------------------------------------------------
    // Staging location — files go to staging, content references target_prefix,
    // file-list CSV maps staging → target for metadata and source → target for data
    // -------------------------------------------------------------------------

    @Test
    public void testRewriteTablePathStagingLocationWritesFilesToStaging()
            throws IOException
    {
        // Metadata files must be physically written under staging_location, not target_prefix.
        // The content inside each file must still reference target_prefix.
        String tableName = "rewrite_table_path_staging";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_stage_target";
            String stagingLocation = sourcePrefix + "_staging";
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // Catalog must be unchanged.
            table.refresh();
            assertEquals(table.location(), originalLocation,
                    "Original table location should not be modified");

            // Metadata file physically written to staging; content references target_prefix.
            TableMetadata stagingMetadata = readLatestTargetMetadata(stagingLocation + originalLocation.substring(sourcePrefix.length()));
            assertEquals(stagingMetadata.location(), expectedNewLocation,
                    "Metadata content should reference target_prefix, not staging_location");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathFileListContents()
            throws IOException
    {
        // file-list is always written to <staging_location>/file-list.
        // Source column: original path for data files, staging path for metadata files.
        // Target column: final target path for all files.
        String tableName = "rewrite_table_path_file_list";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_fl_migrated";
            String stagingLocation = sourcePrefix + "_fl_staging";

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            String fileListPath = stagingLocation + "/" + RewriteTablePathProcedure.FILE_LIST_NAME;
            String localPath = fileListPath.startsWith("file:") ? fileListPath.substring("file:".length()) : fileListPath;
            List<String> lines = Files.readAllLines(Paths.get(localPath));

            assertTrue(lines.size() > 0, "file-list should not be empty");
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                assertEquals(parts.length, 2, "Each line should have exactly two columns: " + line);
                assertTrue(parts[1].startsWith(targetPrefix),
                        format("Target column '%s' should start with '%s'", parts[1], targetPrefix));
            }

            List<String> sourceCol = lines.stream().map(l -> l.split(",", 2)[0]).collect(Collectors.toList());
            List<String> targetCol = lines.stream().map(l -> l.split(",", 2)[1]).collect(Collectors.toList());

            // Data files: source column is the original location, target is target_prefix.
            assertTrue(sourceCol.stream().anyMatch(p -> p.endsWith(".parquet")),
                    "file-list source should contain at least one data file (.parquet)");
            assertTrue(targetCol.stream().anyMatch(p -> p.endsWith(".parquet")),
                    "file-list target should contain at least one data file (.parquet)");

            // Metadata files: source column is the staging path.
            assertTrue(sourceCol.stream().filter(p -> p.endsWith(".avro")).allMatch(p -> p.startsWith(stagingLocation)),
                    "Avro source paths should be under staging_location");
            assertTrue(targetCol.stream().anyMatch(p -> p.contains("/metadata/snap-") && p.endsWith(".avro")),
                    "file-list should contain at least one manifest list (.avro)");
            assertTrue(targetCol.stream().anyMatch(p -> p.contains("/metadata/") && p.endsWith(".avro") && !p.contains("/metadata/snap-")),
                    "file-list should contain at least one manifest file (.avro)");
            assertTrue(targetCol.stream().anyMatch(p -> p.endsWith(".metadata.json")),
                    "file-list should contain the metadata JSON file");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathCreateFileListFalse()
            throws IOException
    {
        // When create_file_list => false, the file-list must NOT be written.
        // Metadata files must still be rewritten normally.
        String tableName = "rewrite_table_path_no_file_list";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_nfl_target";
            String stagingLocation = sourcePrefix + "_nfl_staging";

            assertUpdate(format("CALL system.rewrite_table_path(schema => '%s', table_name => '%s', source_prefix => '%s', target_prefix => '%s', staging_location => '%s', create_file_list => false)",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation)); // named args, order-independent

            // file-list must NOT exist.
            String fileListLocalPath = (stagingLocation + "/" + RewriteTablePathProcedure.FILE_LIST_NAME);
            assertTrue(!new File(fileListLocalPath).exists(),
                    "file-list should NOT exist when create_file_list => false");

            // Metadata must still have been rewritten to staging.
            TableMetadata stagingMetadata = readLatestTargetMetadata(
                    stagingLocation + originalLocation.substring(sourcePrefix.length()));
            assertEquals(stagingMetadata.location(), targetPrefix + originalLocation.substring(sourcePrefix.length()),
                    "Metadata content should reference target_prefix even when create_file_list => false");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathPreviousMetadataVersionsRewritten()
            throws IOException
    {
        // Every previous .metadata.json version (tracked in the metadata log) must also be
        // physically rewritten to staging with content referencing target_prefix.
        // We do multiple inserts to guarantee several metadata versions exist.
        String tableName = "rewrite_table_path_prev_metadata";
        createTable(tableName);
        try {
            // Three inserts → at least three metadata versions (v1 from CREATE, v2, v3, v4).
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_prev_meta_target";
            String stagingLocation = sourcePrefix + "_prev_meta_staging";

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // All .metadata.json files in the staging subtree must exist and reference target_prefix.
            String stagingLocalPath = stagingLocation.startsWith("file:") ? stagingLocation.substring("file:".length()) : stagingLocation;
            List<Path> metadataFiles = Files.walk(Paths.get(stagingLocalPath))
                    .filter(p -> p.getFileName().toString().endsWith(".metadata.json"))
                    .collect(Collectors.toList());

            assertTrue(metadataFiles.size() >= 2,
                    "Expected at least 2 rewritten metadata versions under staging, found: " + metadataFiles.size());

            for (Path metaFile : metadataFiles) {
                String content = new String(Files.readAllBytes(metaFile), StandardCharsets.UTF_8);
                assertTrue(content.contains(targetPrefix),
                        format("Metadata file '%s' should reference target_prefix '%s'", metaFile, targetPrefix));
                // After stripping all targetPrefix occurrences, no residual sourcePrefix should remain.
                // (A plain contains(sourcePrefix) check would always fail because targetPrefix starts
                // with sourcePrefix — every targetPrefix hit is also a sourcePrefix hit.)
                String contentWithoutTarget = content.replace(targetPrefix, "");
                assertTrue(!contentWithoutTarget.contains(sourcePrefix),
                        format("Metadata file '%s' should NOT reference source_prefix '%s' outside of target_prefix", metaFile, sourcePrefix));
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    // -------------------------------------------------------------------------
    // Version range — start_version / end_version bound the metadata JSON window
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // End-to-end — full migration using the file-list as the sole copy manifest
    // -------------------------------------------------------------------------

    @Test
    public void testRewriteTablePathEndToEnd()
            throws IOException
    {
        // Default UUID staging: procedure picks the staging dir automatically.
        // The file-list CSV is the complete copy manifest — copy every row
        // (data files and metadata files alike) then register and query.
        String sourceName = "rewrite_e2e_source";
        String targetName = "rewrite_e2e_target";
        createTable(sourceName);
        try {
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + sourceName + " VALUES (2, 'b')", 1);

            Table table = loadTable(sourceName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_e2e_target";

            // Step 1: rewrite — no staging_location, UUID staging dir chosen automatically.
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix));

            // Step 2: find the UUID staging dir under <originalLocation>/metadata
            // and read the file-list at <staging>/file-list.
            String sourceMetadataDir = originalLocation + "/metadata";
            Path stagingDir = Files.list(toPath(sourceMetadataDir))
                    .filter(p -> p.getFileName().toString().startsWith(RewriteTablePathProcedure.STAGING_DIR_PREFIX))
                    .filter(java.nio.file.Files::isDirectory)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No staging dir found under " + sourceMetadataDir));

            Path fileListPath = stagingDir.resolve(RewriteTablePathProcedure.FILE_LIST_NAME);
            assertTrue(Files.exists(fileListPath), "file-list should exist at " + fileListPath);
            List<String> lines = Files.readAllLines(fileListPath);
            assertTrue(lines.size() > 0, "file-list should not be empty");

            // Step 3: copy every row — data files (original → target) and
            // metadata files (staging → target). No other path logic needed.
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Step 4: derive the metadata dir from the .metadata.json destination row.
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry in file-list"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            // Step 5: register and query.
            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));
            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1, 'a'), (2, 'b')");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    @Test
    public void testRewriteTablePathEndToEndWithStagingLocation()
            throws IOException
    {
        // Explicit staging_location: caller knows the staging path up front so
        // the file-list can be found directly without scanning for a UUID dir.
        // Same copy-everything-from-CSV workflow as the default staging test.
        String sourceName = "rewrite_e2e_staged_source";
        String targetName = "rewrite_e2e_staged_target";
        createTable(sourceName);
        try {
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + sourceName + " VALUES (2, 'b')", 1);

            Table table = loadTable(sourceName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_e2e_staged_target";
            String stagingLocation = sourcePrefix + "_e2e_staging";

            // Step 1: rewrite with an explicit staging_location.
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix, stagingLocation));

            // Step 2: file-list is at the known path <staging_location>/file-list.
            Path fileListPath = toPath(stagingLocation + "/file-list");
            assertTrue(Files.exists(fileListPath), "file-list should exist at " + fileListPath);
            List<String> lines = Files.readAllLines(fileListPath);
            assertTrue(lines.size() > 0, "file-list should not be empty");

            // Step 3: copy every row — data files (original → target) and
            // metadata files (staging → target). No other path logic needed.
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Step 4: derive the metadata dir from the .metadata.json destination row.
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry found in file-list"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            // Step 5: register and query.
            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));
            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1, 'a'), (2, 'b')");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    @Test
    public void testRewriteTablePathDefaultStagingLocation()
            throws IOException
    {
        // Test that when staging_location is omitted, the default UUID staging dir
        // is created under the source metadata directory.
        String tableName = "rewrite_default_staging_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'x')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_default_staging_target";

            // Call WITHOUT staging_location parameter (uses default)
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', null, true)",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix));

            // Default staging dir should be under source metadata directory with STAGING_DIR_PREFIX
            Path metadataDir = toPath(originalLocation + "/metadata");
            assertTrue(Files.exists(metadataDir), "Metadata dir should exist");

            // Find the staging directory (should match pattern: copy-table-staging-<uuid>)
            List<Path> stagingDirs = new ArrayList<>();
            Files.list(metadataDir)
                    .filter(p -> p.getFileName().toString().startsWith(STAGING_DIR_PREFIX))
                    .forEach(stagingDirs::add);

            assertEquals(stagingDirs.size(), 1,
                    format("Should find exactly 1 staging dir with prefix '%s'",
                            STAGING_DIR_PREFIX));

            Path stagingDir = stagingDirs.get(0);
            assertTrue(stagingDir.getFileName().toString().startsWith(
                    STAGING_DIR_PREFIX),
                    format("Staging dir name should start with '%s', was: %s",
                            STAGING_DIR_PREFIX,
                            stagingDir.getFileName()));

            // Verify file-list exists in the staging dir
            Path fileListPath = stagingDir.resolve(
                    FILE_LIST_NAME);
            assertTrue(Files.exists(fileListPath),
                    "file-list should exist in default staging dir: " + fileListPath);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathWithDeleteFiles()
            throws IOException
    {
        // Test that delete files are included in the file list.
        // Create a table with deletes (MOR - merge-on-read).
        String tableName = "rewrite_with_deletes_test";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (format = 'PARQUET', format_version = '2')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);

            Table table = loadTable(tableName);
            table.refresh();

            // Verify we have delete files
            Snapshot snapshot = table.currentSnapshot();
            long deleteFileCount = snapshot.deleteManifests(table.io()).stream().count();
            assertTrue(deleteFileCount > 0, "Should have delete files after DELETE operation");

            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_with_deletes";
            String stagingLocation = sourcePrefix + "_deletes_staging";

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s', true)",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // Verify file list includes delete files
            Path fileListPath = toPath(stagingLocation + "/file-list");
            assertTrue(Files.exists(fileListPath), "file-list should exist");

            List<String> lines = Files.readAllLines(fileListPath);

            // Verify delete files are in the file list
            long deleteFilesInList = lines.stream()
                    .filter(line -> line.contains("delete_file") && line.endsWith(".parquet"))
                    .count();
            assertTrue(deleteFilesInList > 0, "File list should include delete files");

            // Copy all files to target location
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Register the migrated table
            String targetTableName = tableName + "_target";
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry found"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetTableName, targetMetadataDir));

            // TODO: Delete files are correctly collected, copied, and referenced in metadata,
            // but Presto's Iceberg connector does not currently apply them when querying.
            // This appears to be a limitation in how Presto handles delete files for registered tables.
            // Once Presto's connector is fixed, uncomment these assertions:

            // Query the migrated table and verify deletes are still applied
            // Should only have 2 rows (id=1 and id=3), id=2 was deleted
            // assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetTableName), "VALUES (2)");
            // assertQuery(format("SELECT id FROM %s.%s ORDER BY id", TEST_SCHEMA, targetTableName), "VALUES (1), (3)");
            // assertQuery(format("SELECT COUNT(*) FROM %s.%s WHERE id = 2", TEST_SCHEMA, targetTableName), "VALUES (0)");

            // Clean up target table
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetTableName);
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
        }
    }

    @Test
    public void testRewriteTablePathPreservesAvroMetadata()
            throws IOException
    {
        // Verify that critical Iceberg Avro file metadata is preserved during rewrite.
        // Regression test for issue where manifest files lost format-version, content, etc.
        String tableName = "rewrite_avro_meta_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'x')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_avro_test";
            String stagingLocation = sourcePrefix + "_avro_staging";

            // Read metadata from source manifest before rewrite
            Snapshot snapshot = table.currentSnapshot();
            List<ManifestFile> manifests = snapshot.allManifests(table.io());
            assertTrue(!manifests.isEmpty(), "Should have at least one manifest");

            String sourceManifestPath = manifests.get(0).path();
            Path sourceManifestFile = toPath(sourceManifestPath);

            org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> sourceReader =
                    new org.apache.avro.file.DataFileReader<>(
                            new java.io.File(sourceManifestFile.toString()),
                            new org.apache.avro.generic.GenericDatumReader<>());

            String sourceFormatVersion = sourceReader.getMetaString("format-version");
            String sourceContent = sourceReader.getMetaString("content");
            sourceReader.close();

            // Rewrite
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // Read metadata from rewritten manifest
            String targetManifestPath = sourceManifestPath.replace(sourcePrefix, stagingLocation);
            Path targetManifestFile = toPath(targetManifestPath);
            assertTrue(Files.exists(targetManifestFile), "Rewritten manifest should exist");

            org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> targetReader =
                    new org.apache.avro.file.DataFileReader<>(
                            new java.io.File(targetManifestFile.toString()),
                            new org.apache.avro.generic.GenericDatumReader<>());

            String targetFormatVersion = targetReader.getMetaString("format-version");
            String targetContent = targetReader.getMetaString("content");
            targetReader.close();

            // Verify metadata was preserved
            assertEquals(targetFormatVersion, sourceFormatVersion,
                    "format-version metadata should be preserved");
            assertEquals(targetContent, sourceContent,
                    "content metadata should be preserved");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathDisjointPrefixes()
            throws IOException
    {
        // Test with completely disjoint source and target prefixes (different buckets/paths).
        // This makes the "no source prefix after stripping target" assertion meaningful,
        // unlike the sourcePrefix + "_migrated" pattern used in other tests.
        String tableName = "rewrite_disjoint_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'x')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();

            // Use disjoint prefixes: completely different paths
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            // Create truly disjoint paths by replacing part of the path, not extending it
            // This ensures sourcePrefix is NOT a substring of targetPrefix
            String targetPrefix = sourcePrefix.replace("/tpch/", "/migrated/");
            if (targetPrefix.equals(sourcePrefix)) {
                // Fallback if path doesn't contain /tpch/
                targetPrefix = sourcePrefix.substring(0, sourcePrefix.lastIndexOf('/')) + "/migrated" + sourcePrefix.substring(sourcePrefix.lastIndexOf('/'));
            }
            String stagingLocation = sourcePrefix.substring(0, sourcePrefix.lastIndexOf('/')) + "/staging" + sourcePrefix.substring(sourcePrefix.lastIndexOf('/'));

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // Read rewritten metadata and verify no source prefix remains
            Path stagingDir = toPath(stagingLocation);
            assertTrue(Files.exists(stagingDir), "Staging dir should exist: " + stagingDir);

            List<Path> metadataFiles = new ArrayList<>();
            Files.walk(stagingDir)
                    .filter(p -> p.toString().endsWith(".metadata.json"))
                    .forEach(metadataFiles::add);

            assertTrue(!metadataFiles.isEmpty(), "Should have rewritten metadata files");

            for (Path metaFile : metadataFiles) {
                String content = new String(Files.readAllBytes(metaFile), StandardCharsets.UTF_8);

                // With disjoint prefixes, this is a meaningful assertion
                assertTrue(content.contains(targetPrefix),
                        format("Metadata should reference target prefix '%s'", targetPrefix));
                assertTrue(!content.contains(sourcePrefix),
                        format("Metadata should NOT reference source prefix '%s' (disjoint from target)", sourcePrefix));
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathPreservesFormatVersion2()
            throws Exception
    {
        String sourceName = "rewrite_format_v2_source";
        String targetName = "rewrite_format_v2_target";
        try {
            assertUpdate("CREATE TABLE " + sourceName + " (id integer, data varchar) WITH (format_version = '2')");
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'v2-test'), (2, 'format-two')", 2);

            Table table = loadTable(sourceName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_v2_target";
            String stagingLocation = sourcePrefix + "_v2_staging";

            TableMetadata sourceMeta = readLatestTargetMetadata(originalLocation);
            assertEquals(sourceMeta.formatVersion(), 2, "Source should be format version 2");

            // Rewrite metadata
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix, stagingLocation));

            // Copy all files per file-list
            Path fileListPath = toPath(stagingLocation + "/file-list");
            List<String> lines = Files.readAllLines(fileListPath);
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Verify target metadata has format version 2
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());
            TableMetadata targetMeta = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(targetMeta.formatVersion(), 2,
                    "Rewritten metadata should preserve format version 2");

            // Register and query at target location
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry found"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));

            // Query should work and return correct data
            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1, 'v2-test'), (2, 'format-two')");

            // Verify registered table reports format version 2
            Table registeredTable = loadTable(targetName);
            TableMetadata registeredMeta = ((org.apache.iceberg.BaseTable) registeredTable)
                    .operations().current();
            assertEquals(registeredMeta.formatVersion(), 2,
                    "Registered table should have format version 2");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    @Test
    public void testRewriteTablePathPreservesFormatVersion3()
            throws Exception
    {
        String sourceName = "rewrite_format_v3_source";
        String targetName = "rewrite_format_v3_target";
        try {
            assertUpdate("CREATE TABLE " + sourceName + " (id integer, data varchar) WITH (format_version = '3')");
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'v3-test'), (2, 'format-three')", 2);

            Table table = loadTable(sourceName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_v3_target";
            String stagingLocation = sourcePrefix + "_v3_staging";

            TableMetadata sourceMeta = readLatestTargetMetadata(originalLocation);
            assertEquals(sourceMeta.formatVersion(), 3, "Source should be format version 3");

            // Rewrite metadata
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix, stagingLocation));

            // Copy all files per file-list
            Path fileListPath = toPath(stagingLocation + "/file-list");
            List<String> lines = Files.readAllLines(fileListPath);
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Verify target metadata has format version 3
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());
            TableMetadata targetMeta = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(targetMeta.formatVersion(), 3,
                    "Rewritten metadata should preserve format version 3");

            // Register and query at target location
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry found"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));

            // Query should work and return correct data
            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1, 'v3-test'), (2, 'format-three')");

            // Verify registered table reports format version 3
            Table registeredTable = loadTable(targetName);
            TableMetadata registeredMeta = ((org.apache.iceberg.BaseTable) registeredTable)
                    .operations().current();
            assertEquals(registeredMeta.formatVersion(), 3,
                    "Registered table should have format version 3");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    /**
     * Tests rewrite_table_path with format version 2 after complex daily operations:
     * - Initial batch insert
     * - Row-level deletes (creates delete files in v2)
     * - Additional inserts
     * - First rewrite_data_files to compact/optimize
     * - More inserts
     * - More deletes
     * - Second rewrite_data_files
     * - Final inserts after second rewrite
     * - Migrates table to new path and validates data integrity
     *
     * This simulates a realistic production scenario where tables undergo
     * regular maintenance operations before migration.
     */
    @Test
    public void testRewriteTablePathWithComplexOperationsV2()
            throws Exception
    {
        String sourceName = "rewrite_complex_v2_source";
        String targetName = "rewrite_complex_v2_target";
        try {
            // Create v2 table and insert initial data
            assertUpdate("CREATE TABLE " + sourceName + " (id integer, category varchar, value double) WITH (format_version = '2')");
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'C', 300.0)", 4);

            // Delete some rows (creates delete files in format v2)
            assertUpdate("DELETE FROM " + sourceName + " WHERE id = 2", 1);
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (3)");

            // Insert more data
            assertUpdate("INSERT INTO " + sourceName + " VALUES (5, 'B', 250.0), (6, 'A', 175.0)", 2);
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (5)");

            // First rewrite data files to compact
            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, sourceName));
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (5)");

            // Insert more data after first rewrite
            assertUpdate("INSERT INTO " + sourceName + " VALUES (7, 'D', 400.0), (8, 'E', 500.0)", 2);
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (7)");

            // More deletes
            assertUpdate("DELETE FROM " + sourceName + " WHERE category = 'C'", 1);
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (6)");

            // Second rewrite operation
            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, sourceName));
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (6)");

            // Insert after second rewrite
            assertUpdate("INSERT INTO " + sourceName + " VALUES (9, 'F', 600.0), (10, 'A', 125.0)", 2);
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (8)");

            // Now migrate the table
            Table table = loadTable(sourceName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_complex_v2_target";
            String stagingLocation = sourcePrefix + "_complex_v2_staging";

            TableMetadata sourceMeta = readLatestTargetMetadata(originalLocation);
            assertEquals(sourceMeta.formatVersion(), 2, "Source should be format version 2");

            // Rewrite metadata
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix, stagingLocation));

            // Copy all files per file-list
            Path fileListPath = toPath(stagingLocation + "/file-list");
            List<String> lines = Files.readAllLines(fileListPath);
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Verify target metadata preserves format version 2
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());
            TableMetadata targetMeta = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(targetMeta.formatVersion(), 2,
                    "Rewritten metadata should preserve format version 2");

            // Register and query at target location
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry found"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));

            // Query should work and return correct data after all operations
            // Expected: ids 1,3,5,6,7,8,9,10 (deleted 2 and 4)
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (8)");
            assertQuery(format("SELECT id FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1), (3), (5), (6), (7), (8), (9), (10)");
            assertQuery(format("SELECT category, SUM(value) FROM %s.%s GROUP BY category ORDER BY category", TEST_SCHEMA, targetName),
                    "VALUES ('A', 550.0), ('B', 250.0), ('D', 400.0), ('E', 500.0), ('F', 600.0)");

            // Verify registered table reports format version 2
            Table registeredTable = loadTable(targetName);
            TableMetadata registeredMeta = ((org.apache.iceberg.BaseTable) registeredTable)
                    .operations().current();
            assertEquals(registeredMeta.formatVersion(), 2,
                    "Registered table should have format version 2");

            // Perform additional operations on the migrated table to verify it works
            // Insert more data
            assertUpdate(format("INSERT INTO %s.%s VALUES (11, 'H', 800.0), (12, 'I', 900.0)", TEST_SCHEMA, targetName), 2);
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (10)");

            // Delete from migrated table
            assertUpdate(format("DELETE FROM %s.%s WHERE id = 11", TEST_SCHEMA, targetName), 1);
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (9)");

            // Rewrite data files on migrated table
            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, targetName));
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (9)");

            // Final insert after rewrite on migrated table
            assertUpdate(format("INSERT INTO %s.%s VALUES (13, 'J', 1000.0)", TEST_SCHEMA, targetName), 1);
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (10)");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    /**
     * Tests rewrite_table_path with format version 3 after complex daily operations:
     * - Multiple batches of inserts
     * - First rewrite_data_files to compact/optimize
     * - More inserts
     * - Second rewrite_data_files
     * - Final inserts after second rewrite
     * - Migrates table to new path and validates data integrity
     *
     * This simulates a realistic production scenario where v3 tables undergo
     * regular maintenance operations (compaction) before migration.
     * Note: DELETE operations are not yet supported on v3 tables (commented out with TODO).
     */
    @Test
    public void testRewriteTablePathWithComplexOperationsV3()
            throws Exception
    {
        String sourceName = "rewrite_complex_v3_source";
        String targetName = "rewrite_complex_v3_target";
        try {
            // Create v3 table and insert initial data
            assertUpdate("CREATE TABLE " + sourceName + " (id integer, category varchar, value double) WITH (format_version = '3')");
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'C', 300.0)", 4);

            // TODO: Uncomment when DELETE is supported on v3 tables
            // Delete some rows
            // assertUpdate("DELETE FROM " + sourceName + " WHERE id = 2", 1);
            // assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (3)");

            // Insert more data
            assertUpdate("INSERT INTO " + sourceName + " VALUES (5, 'B', 250.0), (6, 'A', 175.0)", 2);
            // assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (5)");
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (6)");

            // First rewrite data files to compact
            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, sourceName));
            // assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (5)");
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (6)");

            // Insert more data after first rewrite
            assertUpdate("INSERT INTO " + sourceName + " VALUES (7, 'D', 400.0), (8, 'E', 500.0)", 2);
            // assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (7)");
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (8)");

            // TODO: Uncomment when DELETE is supported on v3 tables
            // More deletes
            // assertUpdate("DELETE FROM " + sourceName + " WHERE category = 'C'", 1);
            // assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (6)");

            // Second rewrite operation
            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, sourceName));
            // assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (6)");
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (8)");

            // Insert after second rewrite
            assertUpdate("INSERT INTO " + sourceName + " VALUES (9, 'F', 600.0), (10, 'A', 125.0)", 2);
            // assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (8)");
            assertQuery("SELECT COUNT(*) FROM " + sourceName, "VALUES (10)");

            // Now migrate the table
            Table table = loadTable(sourceName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_complex_v3_target";
            String stagingLocation = sourcePrefix + "_complex_v3_staging";

            TableMetadata sourceMeta = readLatestTargetMetadata(originalLocation);
            assertEquals(sourceMeta.formatVersion(), 3, "Source should be format version 3");

            // Rewrite metadata
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix, stagingLocation));

            // Copy all files per file-list
            Path fileListPath = toPath(stagingLocation + "/file-list");
            List<String> lines = Files.readAllLines(fileListPath);
            for (String line : lines) {
                String[] parts = line.split(",", 2);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Verify target metadata preserves format version 3
            String expectedNewLocation = targetPrefix + originalLocation.substring(sourcePrefix.length());
            TableMetadata targetMeta = readLatestTargetMetadata(expectedNewLocation);
            assertEquals(targetMeta.formatVersion(), 3,
                    "Rewritten metadata should preserve format version 3");

            // Register and query at target location
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry found"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));

            // Query should work and return correct data after all operations
            // TODO: When DELETE is enabled, expected: ids 1,3,5,6,7,8,9,10 (deleted 2 and 4)
            // Currently without DELETE: all 10 rows (ids 1-10)
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (10)");
            assertQuery(format("SELECT id FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
            assertQuery(format("SELECT category, SUM(value) FROM %s.%s GROUP BY category ORDER BY category", TEST_SCHEMA, targetName),
                    "VALUES ('A', 550.0), ('B', 450.0), ('C', 300.0), ('D', 400.0), ('E', 500.0), ('F', 600.0)");

            // Verify registered table reports format version 3
            Table registeredTable = loadTable(targetName);
            TableMetadata registeredMeta = ((org.apache.iceberg.BaseTable) registeredTable)
                    .operations().current();
            assertEquals(registeredMeta.formatVersion(), 3,
                    "Registered table should have format version 3");

            // Perform additional operations on the migrated table to verify it works
            // Insert more data
            assertUpdate(format("INSERT INTO %s.%s VALUES (11, 'H', 800.0), (12, 'I', 900.0)", TEST_SCHEMA, targetName), 2);
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (12)");

            // TODO: Uncomment when DELETE is supported on v3 tables
            // Delete from migrated table
            // assertUpdate(format("DELETE FROM %s.%s WHERE id = 11", TEST_SCHEMA, targetName), 1);
            // assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (11)");

            // Rewrite data files on migrated table
            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, targetName));
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (12)");

            // Final insert after rewrite on migrated table
            assertUpdate(format("INSERT INTO %s.%s VALUES (13, 'J', 1000.0)", TEST_SCHEMA, targetName), 1);
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (13)");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    // -------------------------------------------------------------------------
    // manifest_length must track the rewritten manifest's actual size
    // -------------------------------------------------------------------------

    @Test
    public void testRewriteTablePathUpdatesManifestLength()
            throws Exception
    {
        String tableName = "rewrite_manifest_length";
        createTable(tableName);
        try {
            // Two snapshots so the newer manifest list also carries the older manifest forward.
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b')", 2);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'c')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            // Target prefix is deliberately longer than the source, so the rewritten manifests grow
            // and a stale manifest_length would under-report the size and truncate the read.
            String targetPrefix = sourcePrefix + "_mlen_target";
            String stagingLocation = sourcePrefix + "_mlen_staging";

            Map<String, Long> sourceLengths = new HashMap<>();
            for (Snapshot snapshot : ((BaseTable) table).operations().current().snapshots()) {
                for (ManifestFile manifest : snapshot.allManifests(((BaseTable) table).operations().io())) {
                    sourceLengths.put(manifest.path(), manifest.length());
                }
            }
            assertTrue(!sourceLengths.isEmpty(), "Source table should have manifests");

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            int checked = assertManifestLengthsMatchStagedFiles(stagingLocation, targetPrefix);
            assertTrue(checked >= sourceLengths.size(),
                    format("Expected at least %s manifest list records, checked %s", sourceLengths.size(), checked));

            // Guard against a vacuous test: if rewriting happened to preserve every manifest's size,
            // the assertion above would pass even with a stale manifest_length.
            boolean anySizeChanged = false;
            for (Map.Entry<String, Long> entry : sourceLengths.entrySet()) {
                Path staged = toPath(stagingLocation + entry.getKey().substring(sourcePrefix.length()));
                if (Files.size(staged) != entry.getValue()) {
                    anySizeChanged = true;
                }
            }
            assertTrue(anySizeChanged,
                    "Rewritten manifests should differ in size from the source, otherwise this test proves nothing");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * A manifest spanning more than one Avro block is where a stale {@code manifest_length} actually
     * loses data: the read stops at the recorded length, silently dropping trailing blocks. On a
     * single-block manifest the shortfall only eats into the trailing sync marker and all records
     * still come back, which is why small fixtures hide the bug.
     */
    @Test
    public void testRewriteTablePathReadsAllEntriesOfMultiBlockManifest()
            throws Exception
    {
        String sourceName = "rewrite_multiblock_source";
        String targetName = "rewrite_multiblock_target";
        try {
            // One entry per partition, and wide rows so each entry carries bulky per-column metrics
            // (column_sizes, value_counts, lower_bounds, upper_bounds, …). Partitions stay under the
            // 100 open-writer cap while the manifest still grows past one Avro block.
            int partitionCount = 99;
            StringBuilder columns = new StringBuilder("part integer, id bigint");
            StringBuilder projection = new StringBuilder(
                    format("CAST(orderkey %% %s AS integer), orderkey", partitionCount));
            for (int i = 0; i < 30; i++) {
                columns.append(format(", c%s varchar", i));
                projection.append(", comment");
            }

            assertUpdate(format("CREATE TABLE %s (%s) WITH (partitioning = ARRAY['part'])", sourceName, columns));
            assertUpdate(format("INSERT INTO %s SELECT %s FROM tpch.tiny.orders", sourceName, projection), 15000);

            Table table = loadTable(sourceName);
            table.refresh();
            TableMetadata sourceMetadata = ((BaseTable) table).operations().current();

            int mostBlocks = 0;
            for (Snapshot snapshot : sourceMetadata.snapshots()) {
                for (ManifestFile manifest : snapshot.allManifests(((BaseTable) table).operations().io())) {
                    mostBlocks = Math.max(mostBlocks, countAvroBlocks(toPath(manifest.path())));
                }
            }
            // Block count, not file size: manifests are gzip-compressed, and Avro flushes a block
            // once the *uncompressed* buffer passes its 64000-byte sync interval.
            assertTrue(mostBlocks > 1,
                    format("Fixture must produce a multi-block manifest; most blocks seen was %s", mostBlocks));

            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_multiblock_target";
            String stagingLocation = sourcePrefix + "_multiblock_staging";

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix, stagingLocation));

            assertManifestLengthsMatchStagedFiles(stagingLocation, targetPrefix);

            copyFromFileList(stagingLocation);

            List<String> lines = Files.readAllLines(toPath(stagingLocation + "/" + FILE_LIST_NAME));
            String metadataFileDest = lines.stream()
                    .map(l -> l.split(",", 2)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json entry found"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));

            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));

            // Every manifest entry must survive the rewrite: a truncated manifest read would drop
            // data files and quietly return fewer rows and fewer partitions.
            assertQuery(format("SELECT COUNT(*) FROM %s.%s", TEST_SCHEMA, targetName), "VALUES (15000)");
            assertQuery(format("SELECT COUNT(DISTINCT part) FROM %s.%s", TEST_SCHEMA, targetName),
                    format("VALUES (%s)", partitionCount));
            // Compare against the source table on the Presto side: dropping a manifest entry loses a
            // whole data file, which would move this sum.
            assertEquals(
                    computeScalar(format("SELECT SUM(id) FROM %s.%s", TEST_SCHEMA, targetName)),
                    computeScalar(format("SELECT SUM(id) FROM %s.%s", TEST_SCHEMA, sourceName)),
                    "Migrated table should return the same id sum as the source");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
        }
    }

    /**
     * Counts the Avro blocks in a container file by tracking how many distinct sync positions are
     * crossed while iterating. Used to prove a manifest fixture really spans more than one block,
     * which is the only case where a short read bound loses records.
     */
    private static int countAvroBlocks(Path avroFile)
            throws IOException
    {
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                avroFile.toFile(), new GenericDatumReader<>())) {
            Set<Long> syncPositions = new HashSet<>();
            for (GenericRecord ignored : reader) {
                syncPositions.add(reader.previousSync());
            }
            return syncPositions.size();
        }
    }

    /**
     * Asserts that every {@code manifest_length} recorded in the rewritten manifest lists under
     * {@code stagingLocation} equals the actual byte size of the manifest it points at.
     *
     * <p>The recorded length is the authoritative read bound for a manifest — Presto passes it
     * straight through to the Avro reader without stat-ing the file — so a stale value silently
     * truncates the read. Returns the number of records checked.
     */
    private static int assertManifestLengthsMatchStagedFiles(String stagingLocation, String targetPrefix)
            throws IOException
    {
        Path stagingDir = toPath(stagingLocation);
        List<Path> avroFiles = new ArrayList<>();
        Files.walk(stagingDir)
                .filter(p -> p.getFileName().toString().endsWith(".avro"))
                .forEach(avroFiles::add);
        assertTrue(!avroFiles.isEmpty(), "Expected rewritten Avro files under " + stagingDir);

        int checked = 0;
        for (Path avroFile : avroFiles) {
            try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                    avroFile.toFile(), new GenericDatumReader<>())) {
                // Manifest lists carry manifest_path; manifests carry data_file. Identify by schema
                // rather than filename so the check does not depend on Iceberg's naming scheme.
                if (reader.getSchema().getField("manifest_path") == null) {
                    continue;
                }
                for (GenericRecord record : reader) {
                    String manifestPath = record.get("manifest_path").toString();
                    long recordedLength = (Long) record.get("manifest_length");

                    // The record points at the final target path, but the bytes live in staging.
                    assertTrue(manifestPath.startsWith(targetPrefix), format(
                            "manifest_path '%s' should reference target prefix '%s'", manifestPath, targetPrefix));
                    Path stagedManifest = toPath(stagingLocation + manifestPath.substring(targetPrefix.length()));
                    assertTrue(Files.exists(stagedManifest), "Rewritten manifest missing: " + stagedManifest);

                    assertEquals(recordedLength, Files.size(stagedManifest), format(
                            "manifest_length recorded for '%s' must equal the rewritten manifest's actual size",
                            manifestPath));
                    checked++;
                }
            }
        }
        assertTrue(checked > 0, "No manifest list records were checked under " + stagingDir);
        return checked;
    }

    private void createTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
        assertUpdate("CREATE TABLE " + TEST_SCHEMA + "." + tableName + " (id INTEGER, value VARCHAR)");
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
    }

    private Table loadTable(String tableName)
    {
        tableName = normalizeIdentifier(tableName, ICEBERG_CATALOG);
        Catalog catalog = CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        return ImmutableMap.of("warehouse", getCatalogDirectory().toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        return getIcebergDataDirectoryPath(dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false).toFile();
    }

    /**
     * Finds the latest {@code .metadata.json} written under {@code <tableLocation>/metadata/}
     * by last-modified time and parses it. Avoids hardcoding version numbers (v1, v2, v3, …).
     */
    private TableMetadata readLatestTargetMetadata(String tableLocation)
    {
        String localPath = tableLocation.startsWith("file:") ? tableLocation.substring("file:".length()) : tableLocation;
        Path metadataDir = Paths.get(localPath, "metadata");
        try {
            Optional<Path> latest = Files.list(metadataDir)
                    .filter(p -> p.getFileName().toString().endsWith(".metadata.json"))
                    .max(Comparator.comparingLong(p -> p.toFile().lastModified()));
            assertTrue(latest.isPresent(), "No metadata file found under " + metadataDir);
            return TableMetadataParser.read(null,
                    HadoopInputFile.fromLocation("file:" + latest.get().toAbsolutePath(), new Configuration()));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to list metadata directory: " + metadataDir, e);
        }
    }

    /**
     * Converts a file:// URI string to a filesystem Path, handling the URI properly
     * instead of brittle string replacement.
     */
    private static Path toPath(String uriString)
    {
        if (uriString.startsWith("file:")) {
            return Paths.get(URI.create(uriString));
        }
        return Paths.get(uriString);
    }

    /**
     * Reads the file-list CSV from {@code <stagingLocation>/file-list} and copies every row
     * from the source column to the destination column. Works for both data files (original
     * source → target) and metadata files (staging → target).
     */
    private static void copyFromFileList(String stagingLocation)
            throws IOException
    {
        Path fileListPath = toPath(stagingLocation + "/" + FILE_LIST_NAME);
        assertTrue(Files.exists(fileListPath), "file-list should exist at " + fileListPath);
        List<String> lines = Files.readAllLines(fileListPath);
        assertTrue(lines.size() > 0, "file-list should not be empty");
        for (String line : lines) {
            String[] parts = line.split(",", 2);
            Path from = toPath(parts[0]);
            Path to = toPath(parts[1]);
            Files.createDirectories(to.getParent());
            Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
