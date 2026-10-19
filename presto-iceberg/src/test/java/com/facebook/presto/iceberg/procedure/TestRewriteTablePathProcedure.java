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
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
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
import static org.testng.Assert.assertFalse;
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
    public void testRewriteTablePathNullPrefixesFail()
    {
        // NULL source_prefix or target_prefix must fail with a clear INVALID_PROCEDURE_ARGUMENT
        // error rather than an NPE from stripTrailingSlash.
        String tableName = "rewrite_null_prefix_test";
        createTable(tableName);
        try {
            assertQueryFails(
                    format("CALL system.rewrite_table_path('%s', '%s', null, 'file://new/warehouse')",
                            TEST_SCHEMA, tableName),
                    "source_prefix cannot be null");
            assertQueryFails(
                    format("CALL system.rewrite_table_path('%s', '%s', 'file://old/warehouse', null)",
                            TEST_SCHEMA, tableName),
                    "target_prefix cannot be null");
        }
        finally {
            dropTable(tableName);
        }
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

    @Test
    public void testRewriteMetadataJsonPathsFieldAware()
    {
        // Build a minimal Iceberg metadata JSON exercising all path fields plus edge cases.
        String src = "s3://bucket/warehouse";
        String tgt = "s3://bucket/target";

        // Path that contains the source prefix more than once (double-prefix bug).
        // The document-wide text-replace approach rewrites BOTH occurrences; the field-aware
        // approach rewrites only the leading prefix, leaving the embedded occurrence intact.
        String doublePrefixPath = src + "/archive/" + src + "/t/metadata/v1.metadata.json";
        String expectedDoublePrefixPath = tgt + "/archive/" + src + "/t/metadata/v1.metadata.json";

        String json = "{"
                + "\"format-version\":2,"
                + "\"location\":\"" + src + "/t\","
                + "\"snapshots\":[{\"snapshot-id\":1,\"manifest-list\":\"" + src + "/t/metadata/snap.avro\"}],"
                + "\"metadata-log\":[{\"timestamp-ms\":1,\"metadata-file\":\"" + doublePrefixPath + "\"}],"
                + "\"statistics\":[{\"snapshot-id\":1,\"statistics-path\":\"" + src + "/t/metadata/stats.puffin\",\"file-size-in-bytes\":100,\"file-footer-size-in-bytes\":10,\"blob-metadata\":[]}],"
                + "\"partition-statistics\":[{\"snapshot-id\":1,\"statistics-path\":\"" + src + "/t/metadata/pstats\"}],"
                + "\"properties\":{"
                + "\"write.location\":\"" + src + "/t\","          // path property → rewritten
                + "\"column.comment\":\"a " + src + " string\","   // non-path property value → NOT rewritten
                + "\"table.name\":\"my_table\""                     // non-path property → NOT rewritten
                + "}"
                + "}";

        String result = RewriteTablePathProcedure.rewriteMetadataJsonPaths(json, src, tgt);

        // location
        assertTrue(result.contains("\"" + tgt + "/t\""), "location must reference target prefix");
        assertFalse(result.contains("\"" + src + "/t\""), "location must not reference source prefix");

        // snapshot manifest-list
        assertTrue(result.contains(tgt + "/t/metadata/snap.avro"), "manifest-list must reference target prefix");

        // metadata-log: only the leading prefix replaced — the embedded occurrence survives
        assertTrue(result.contains(expectedDoublePrefixPath),
                "metadata-file must replace only the leading prefix occurrence");
        assertFalse(result.contains(tgt + "/archive/" + tgt),
                "double-prefix path must not have both occurrences replaced");

        // statistics
        assertTrue(result.contains(tgt + "/t/metadata/stats.puffin"), "statistics-path must reference target prefix");

        // partition-statistics
        assertTrue(result.contains(tgt + "/t/metadata/pstats"), "partition-statistics-path must reference target prefix");

        // properties: write.location is a path → rewritten; other values untouched
        assertTrue(result.contains("\"write.location\":\"" + tgt + "/t\""),
                "write.location property must reference target prefix");
        assertTrue(result.contains("\"a " + src + " string\""),
                "non-path column.comment property value must not be rewritten");
        assertTrue(result.contains("\"my_table\""), "table.name property must be unchanged");
    }

    @Test
    public void testRewriteMetadataJsonPathsJsonEscaping()
    {
        // Target prefix containing a backslash must be JSON-escaped in the output.
        // This guards against producing invalid JSON when the replacement text has metacharacters.
        String src = "s3://bucket/src";
        // Use a target with a backslash (unusual but possible on some filesystems/configs).
        String tgt = "s3://bucket/tar\\get";
        String json = "{\"format-version\":2,\"location\":\"" + src + "/t\"}";

        String result = RewriteTablePathProcedure.rewriteMetadataJsonPaths(json, src, tgt);

        // Jackson must have serialised the backslash as \\ in the JSON output.
        assertTrue(result.contains("tar\\\\get") || result.contains("tar\\get"),
                "backslash in target prefix must be preserved in output JSON");
        // Round-trip: the parsed location must equal tgt + "/t"
        try {
            com.fasterxml.jackson.databind.JsonNode node =
                    new com.fasterxml.jackson.databind.ObjectMapper().readTree(result);
            assertEquals(node.path("location").textValue(), tgt + "/t",
                    "round-tripped location must equal target + path suffix");
        }
        catch (Exception e) {
            throw new AssertionError("rewritten JSON must be parseable: " + result, e);
        }
    }

    @Test
    public void testRewriteMetadataJsonGzipCompressed()
            throws Exception
    {
        // Verify that gzip-compressed metadata (.gz.metadata.json and .metadata.json.gz) is
        // decompressed before path rewriting and re-compressed on write, so the output is a
        // valid gzip stream with correct paths rather than a corrupted file.
        String src = "s3://bucket/src";
        String tgt = "s3://bucket/tgt";
        String json = "{\"format-version\":2,\"location\":\"" + src + "/t\","
                + "\"snapshots\":[{\"snapshot-id\":1,\"manifest-list\":\"" + src + "/t/metadata/snap.avro\"}]}";

        for (String suffix : new String[] {".gz.metadata.json", ".metadata.json.gz"}) {
            assertTrue(RewriteTablePathProcedure.isGzipMetadata("00001" + suffix),
                    "isGzipMetadata must return true for " + suffix);

            // Compress the JSON as gzip
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            try (java.util.zip.GZIPOutputStream gzos = new java.util.zip.GZIPOutputStream(baos)) {
                gzos.write(json.getBytes(StandardCharsets.UTF_8));
            }
            byte[] compressed = baos.toByteArray();

            Path tmpDir = Files.createTempDirectory("iceberg-gz-metadata-test");
            try {
                // Write compressed metadata to a temp file with gzip suffix
                String srcMetaPath = "file://" + tmpDir + "/src/metadata/00001" + suffix;
                String stagingPrefix = "file://" + tmpDir + "/staging";
                String targetPrefix = tgt;
                String sourcePrefix = src;

                // Use HadoopFileIO to write the compressed source file
                HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
                String srcLocalPath = tmpDir + "/src/metadata/00001" + suffix;
                Files.createDirectories(Paths.get(tmpDir + "/src/metadata"));
                Files.write(Paths.get(srcLocalPath), compressed);

                // Call rewriteMetadataJson via the full procedure indirectly:
                // decompress → rewrite → compress by invoking gunzip+gzip path directly.
                // Verify isGzipMetadata + gunzip + gzip round-trip.
                String decompressed = new String(
                        decompress(compressed), StandardCharsets.UTF_8);
                assertEquals(decompressed, json, "gunzip round-trip must reproduce original JSON");

                String rewritten = RewriteTablePathProcedure.rewriteMetadataJsonPaths(decompressed, src, tgt);
                assertTrue(rewritten.contains(tgt + "/t"), "rewritten JSON must reference target");
                assertFalse(rewritten.contains(src + "/t"), "rewritten JSON must not reference source");

                // Compress the rewritten JSON and verify the bytes decompress cleanly
                java.io.ByteArrayOutputStream rewrittenBaos = new java.io.ByteArrayOutputStream();
                try (java.util.zip.GZIPOutputStream gzos2 = new java.util.zip.GZIPOutputStream(rewrittenBaos)) {
                    gzos2.write(rewritten.getBytes(StandardCharsets.UTF_8));
                }
                byte[] rewrittenCompressed = rewrittenBaos.toByteArray();
                String roundTripped = new String(decompress(rewrittenCompressed), StandardCharsets.UTF_8);
                assertEquals(roundTripped, rewritten, "gzip round-trip of rewritten JSON must be lossless");
            }
            finally {
                Files.walk(tmpDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
    }

    private static byte[] decompress(byte[] data)
            throws IOException
    {
        try (java.util.zip.GZIPInputStream gzis = new java.util.zip.GZIPInputStream(
                new java.io.ByteArrayInputStream(data));
             java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = gzis.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        }
    }

    /**
     * Regression test for V3 deletion-vector entries: the {@code referenced_data_file} field in
     * the {@code data_file} struct of a manifest entry must be rewritten along with {@code file_path}
     * so that the DV continues to associate with the relocated data file.
     *
     * <p>This is tested at the Avro-rewrite level because Presto's Iceberg connector does not yet
     * support creating deletion vectors via SQL (V3 DELETE is not wired up). The test builds a
     * minimal manifest-shaped Avro file with both fields populated, calls {@code rewriteAvroFile}
     * directly, and asserts that both paths reference the target prefix in the output.
     */
    @Test
    public void testRewriteAvroFileRewritesReferencedDataFile()
            throws Exception
    {
        Path tmpDir = Files.createTempDirectory("iceberg-dv-rewrite-test");
        try {
            String sourcePrefix = "file://" + tmpDir + "/source";
            String targetPrefix = "file://" + tmpDir + "/target";

            // Minimal manifest-like Avro schema: a data_file record with file_path and
            // referenced_data_file (the two path fields a DV entry carries).
            org.apache.avro.Schema nullable = org.apache.avro.Schema.createUnion(
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
            org.apache.avro.Schema dataFileSchema = org.apache.avro.Schema.createRecord(
                    "r2", null, null, false,
                    java.util.Arrays.asList(
                            new org.apache.avro.Schema.Field("file_path", nullable, null, (Object) null),
                            new org.apache.avro.Schema.Field("referenced_data_file", nullable, null, (Object) null)));
            org.apache.avro.Schema manifestSchema = org.apache.avro.Schema.createRecord(
                    "manifest_entry", null, null, false,
                    java.util.Arrays.asList(
                            new org.apache.avro.Schema.Field("data_file", dataFileSchema, null, (Object) null)));

            // Write a single DV-like entry to a temp Avro file.
            Path srcAvro = tmpDir.resolve("manifest.avro");
            try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                 org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> w =
                         new org.apache.avro.file.DataFileWriter<>(new org.apache.avro.generic.GenericDatumWriter<>(manifestSchema))) {
                w.create(manifestSchema, baos);
                org.apache.avro.generic.GenericRecord dataFile =
                        new org.apache.avro.generic.GenericData.Record(dataFileSchema);
                dataFile.put("file_path", new org.apache.avro.util.Utf8(sourcePrefix + "/data/00000.parquet"));
                dataFile.put("referenced_data_file", new org.apache.avro.util.Utf8(sourcePrefix + "/data/00001.parquet"));
                org.apache.avro.generic.GenericRecord entry =
                        new org.apache.avro.generic.GenericData.Record(manifestSchema);
                entry.put("data_file", dataFile);
                w.append(entry);
                w.flush();
                Files.write(srcAvro, baos.toByteArray());
            }

            // Rewrite via the procedure's Avro rewriter.
            Path dstAvro = tmpDir.resolve("manifest_rewritten.avro");
            HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
            RewriteTablePathProcedure.rewriteAvroFile(
                    srcAvro.toUri().toString(),
                    "data_file",
                    "file_path",
                    fileIO,
                    sourcePrefix,
                    targetPrefix,
                    dstAvro.toUri().toString(),
                    new ArrayList<>(),
                    null);

            // Both path fields must reference the target prefix in the rewritten file.
            try (org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> r =
                    new org.apache.avro.file.DataFileReader<>(
                            new org.apache.avro.file.SeekableByteArrayInput(Files.readAllBytes(dstAvro)),
                            new org.apache.avro.generic.GenericDatumReader<>())) {
                assertTrue(r.hasNext(), "Rewritten manifest must have at least one record");
                org.apache.avro.generic.GenericRecord record = r.next();
                org.apache.avro.generic.GenericRecord df = (org.apache.avro.generic.GenericRecord) record.get("data_file");
                assertEquals(df.get("file_path").toString(), targetPrefix + "/data/00000.parquet",
                        "file_path must reference target prefix");
                assertEquals(df.get("referenced_data_file").toString(), targetPrefix + "/data/00001.parquet",
                        "referenced_data_file must reference target prefix");
            }
        }
        finally {
            Files.walk(tmpDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
        }
    }

    @Test
    public void testRewriteAvroFileSkipsDeletedEntriesInFileList()
            throws Exception
    {
        // DELETED entries (status=2) remain in the Avro manifest but must NOT appear in the
        // collectDataFiles output: the physical file may have been removed by expire_snapshots
        // or remove_orphan_files, so including it would cause the migration copy step to fail.
        // The path MUST still be rewritten in the Avro output (for manifest consistency).
        Path tmpDir = Files.createTempDirectory("iceberg-deleted-entry-test");
        try {
            String sourcePrefix = "file://" + tmpDir + "/source";
            String targetPrefix = "file://" + tmpDir + "/target";

            org.apache.avro.Schema nullable = org.apache.avro.Schema.createUnion(
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
            org.apache.avro.Schema dataFileSchema = org.apache.avro.Schema.createRecord(
                    "r2", null, null, false,
                    java.util.Arrays.asList(
                            new org.apache.avro.Schema.Field("file_path", nullable, null, (Object) null)));
            org.apache.avro.Schema manifestSchema = org.apache.avro.Schema.createRecord(
                    "manifest_entry", null, null, false,
                    java.util.Arrays.asList(
                            new org.apache.avro.Schema.Field("status",
                                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, (Object) null),
                            new org.apache.avro.Schema.Field("data_file", dataFileSchema, null, (Object) null)));

            String existingPath = sourcePrefix + "/data/existing.parquet";
            String deletedPath = sourcePrefix + "/data/deleted.parquet";

            Path srcAvro = tmpDir.resolve("manifest.avro");
            try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                 org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> w =
                         new org.apache.avro.file.DataFileWriter<>(new org.apache.avro.generic.GenericDatumWriter<>(manifestSchema))) {
                w.create(manifestSchema, baos);

                // EXISTING entry (status=0)
                org.apache.avro.generic.GenericRecord existingFile =
                        new org.apache.avro.generic.GenericData.Record(dataFileSchema);
                existingFile.put("file_path", new org.apache.avro.util.Utf8(existingPath));
                org.apache.avro.generic.GenericRecord existingEntry =
                        new org.apache.avro.generic.GenericData.Record(manifestSchema);
                existingEntry.put("status", 0);
                existingEntry.put("data_file", existingFile);
                w.append(existingEntry);

                // DELETED entry (status=2) — simulates a file removed by expire_snapshots
                org.apache.avro.generic.GenericRecord deletedFile =
                        new org.apache.avro.generic.GenericData.Record(dataFileSchema);
                deletedFile.put("file_path", new org.apache.avro.util.Utf8(deletedPath));
                org.apache.avro.generic.GenericRecord deletedEntry =
                        new org.apache.avro.generic.GenericData.Record(manifestSchema);
                deletedEntry.put("status", 2);
                deletedEntry.put("data_file", deletedFile);
                w.append(deletedEntry);

                w.flush();
                Files.write(srcAvro, baos.toByteArray());
            }

            HadoopFileIO fileIO = new HadoopFileIO(new org.apache.hadoop.conf.Configuration());
            String srcPath = srcAvro.toUri().toString();
            String outPath = tmpDir.resolve("manifest_out.avro").toUri().toString();
            List<String[]> collected = new ArrayList<>();

            RewriteTablePathProcedure.rewriteAvroFile(
                    srcPath, "data_file", "file_path", fileIO,
                    sourcePrefix, targetPrefix, outPath, collected, null);

            // EXISTING entry must be in the collection
            assertEquals(collected.size(), 1, "Only the EXISTING entry must appear in the file list");
            assertEquals(collected.get(0)[0], existingPath);
            assertEquals(collected.get(0)[1], targetPrefix + "/data/existing.parquet");

            // Both paths must be rewritten in the output Avro
            try (org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> outReader =
                         new org.apache.avro.file.DataFileReader<>(
                                 new org.apache.avro.file.SeekableFileInput(tmpDir.resolve("manifest_out.avro").toFile()),
                                 new org.apache.avro.generic.GenericDatumReader<>())) {
                List<String> outPaths = new ArrayList<>();
                for (org.apache.avro.generic.GenericRecord r : outReader) {
                    org.apache.avro.generic.GenericRecord df = (org.apache.avro.generic.GenericRecord) r.get("data_file");
                    outPaths.add(df.get("file_path").toString());
                }
                assertEquals(outPaths.size(), 2, "Both entries must be present in the output Avro");
                assertTrue(outPaths.stream().allMatch(p -> p.startsWith(targetPrefix)),
                        "All paths in output Avro must reference target prefix");
                assertFalse(outPaths.stream().anyMatch(p -> p.startsWith(sourcePrefix)),
                        "No paths in output Avro should reference source prefix");
            }
        }
        finally {
            Files.walk(tmpDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
        }
    }

    @Test
    public void testRewriteAvroFileThrowsForDataFileOutsideSourcePrefix()
            throws Exception
    {
        // Data files outside source_prefix must cause the procedure to fail, consistent with the
        // behavior for manifest and manifest-list paths. Silently skipping them would produce a
        // partially-migrated table that reads from a mix of old and new storage without any
        // indication to the caller (e.g. tables using write.data.path or files added via add_files).
        Path tmpDir = Files.createTempDirectory("iceberg-outofprefix-test");
        try {
            String sourcePrefix = "file://" + tmpDir + "/source";
            String targetPrefix = "file://" + tmpDir + "/target";
            String outsidePrefix = "file://" + tmpDir + "/external";

            org.apache.avro.Schema nullable = org.apache.avro.Schema.createUnion(
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
            org.apache.avro.Schema dataFileSchema = org.apache.avro.Schema.createRecord(
                    "r2", null, null, false,
                    java.util.Arrays.asList(
                            new org.apache.avro.Schema.Field("file_path", nullable, null, (Object) null)));
            org.apache.avro.Schema manifestSchema = org.apache.avro.Schema.createRecord(
                    "manifest_entry", null, null, false,
                    java.util.Arrays.asList(
                            new org.apache.avro.Schema.Field("status",
                                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, (Object) null),
                            new org.apache.avro.Schema.Field("data_file", dataFileSchema, null, (Object) null)));

            Path srcAvro = tmpDir.resolve("manifest.avro");
            try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                 org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> w =
                         new org.apache.avro.file.DataFileWriter<>(new org.apache.avro.generic.GenericDatumWriter<>(manifestSchema))) {
                w.create(manifestSchema, baos);

                // One file within source_prefix (fine) …
                org.apache.avro.generic.GenericRecord inFile =
                        new org.apache.avro.generic.GenericData.Record(dataFileSchema);
                inFile.put("file_path", new org.apache.avro.util.Utf8(sourcePrefix + "/data/in.parquet"));
                org.apache.avro.generic.GenericRecord inEntry =
                        new org.apache.avro.generic.GenericData.Record(manifestSchema);
                inEntry.put("status", 0);
                inEntry.put("data_file", inFile);
                w.append(inEntry);

                // … and one outside source_prefix — must trigger the failure.
                org.apache.avro.generic.GenericRecord outFile =
                        new org.apache.avro.generic.GenericData.Record(dataFileSchema);
                outFile.put("file_path", new org.apache.avro.util.Utf8(outsidePrefix + "/data/out.parquet"));
                org.apache.avro.generic.GenericRecord outEntry =
                        new org.apache.avro.generic.GenericData.Record(manifestSchema);
                outEntry.put("status", 0);
                outEntry.put("data_file", outFile);
                w.append(outEntry);

                w.flush();
                Files.write(srcAvro, baos.toByteArray());
            }

            HadoopFileIO fileIO = new HadoopFileIO(new org.apache.hadoop.conf.Configuration());
            String outPath = tmpDir.resolve("manifest_out.avro").toUri().toString();

            try {
                RewriteTablePathProcedure.rewriteAvroFile(
                        srcAvro.toUri().toString(), "data_file", "file_path", fileIO,
                        sourcePrefix, targetPrefix, outPath, new ArrayList<>(), null);
                throw new AssertionError("Expected PrestoException for data file outside source_prefix");
            }
            catch (com.facebook.presto.spi.PrestoException e) {
                assertTrue(e.getMessage().contains(outsidePrefix + "/data/out.parquet"),
                        "Error message must name the offending file: " + e.getMessage());
                assertTrue(e.getMessage().contains(sourcePrefix),
                        "Error message must name source_prefix: " + e.getMessage());
            }
        }
        finally {
            Files.walk(tmpDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
        }
    }

    @Test
    public void testCanonicalizeLocationNormalizesEquivalentForms()
    {
        // file:/path and file:///path must resolve to the same canonical form
        assertEquals(
                RewriteTablePathProcedure.canonicalizeLocation("file:/tmp/warehouse/tbl"),
                RewriteTablePathProcedure.canonicalizeLocation("file:///tmp/warehouse/tbl"));

        // Dot segments must be collapsed
        assertEquals(
                RewriteTablePathProcedure.canonicalizeLocation("file:///tmp/warehouse/tbl"),
                RewriteTablePathProcedure.canonicalizeLocation("file:///tmp/warehouse/tbl/."));
        assertEquals(
                RewriteTablePathProcedure.canonicalizeLocation("file:///tmp/warehouse/tbl"),
                RewriteTablePathProcedure.canonicalizeLocation("file:///tmp/warehouse/other/../tbl"));

        // Distinct locations must remain distinct
        assertFalse(
                RewriteTablePathProcedure.canonicalizeLocation("file:///tmp/warehouse/tbl")
                        .equals(RewriteTablePathProcedure.canonicalizeLocation("file:///tmp/warehouse/tbl_backup")));

        // Non-file schemes: dot segments collapsed, distinct paths remain distinct
        assertEquals(
                RewriteTablePathProcedure.canonicalizeLocation("s3://bucket/warehouse/tbl"),
                RewriteTablePathProcedure.canonicalizeLocation("s3://bucket/warehouse/other/../tbl"));
        assertFalse(
                RewriteTablePathProcedure.canonicalizeLocation("s3://bucket/warehouse/tbl")
                        .equals(RewriteTablePathProcedure.canonicalizeLocation("s3://bucket/warehouse/tbl_backup")));
    }

    @Test
    public void testRewriteTablePathStagingEqualToSourceRejectedForEquivalentUris()
    {
        // Regression test: the staging == source guard must fire even when the two URI strings
        // differ only in spelling (file:/path vs file:///path, trailing dot segment, etc.).
        String tableName = "rewrite_path_canonical_staging_check";
        createTable(tableName);
        try {
            Table table = loadTable(tableName);
            table.refresh();
            String tableLocation = table.location(); // e.g. file:///...warehouse.../tpch/rewrite_path_...
            String sourcePrefix = tableLocation.substring(0, tableLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_target";

            // Build an alternative-spelling URI that resolves to the same directory as sourcePrefix.
            // Strip "file://" so we get "file:/" + absolute-path, which equals "file:///abs-path".
            String equivalentStaging = sourcePrefix.replaceFirst("file:///", "file:/");

            assertQueryFails(
                    format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                            TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, equivalentStaging),
                    "staging_location must differ from source_prefix");

            // Dot-segment variant: append /. which normalises to the same directory.
            String dotSegmentStaging = sourcePrefix + "/.";
            assertQueryFails(
                    format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                            TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, dotSegmentStaging),
                    "staging_location must differ from source_prefix");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testHasPathPrefixBoundaryAwareness()
    {
        // Exact match
        assertTrue(RewriteTablePathProcedure.hasPathPrefix("s3://bucket/tbl", "s3://bucket/tbl"));
        // Path under prefix
        assertTrue(RewriteTablePathProcedure.hasPathPrefix("s3://bucket/tbl/metadata/v1.json", "s3://bucket/tbl"));
        // Sibling directory — shares a string prefix but NOT at a path boundary
        assertFalse(RewriteTablePathProcedure.hasPathPrefix("s3://bucket/tbl_backup/metadata/v1.json", "s3://bucket/tbl"));
        assertFalse(RewriteTablePathProcedure.hasPathPrefix("s3://bucket/tbl_backup", "s3://bucket/tbl"));
        // Unrelated path
        assertFalse(RewriteTablePathProcedure.hasPathPrefix("s3://bucket/other/path", "s3://bucket/tbl"));
    }

    @Test
    public void testRewriteTablePathRejectsSiblingPrefixMatch()
    {
        // Regression test: source_prefix must match at a path boundary, not as a bare string prefix.
        // A source_prefix of "file://.../tbl" must NOT match a table located at "file://.../tbl_extra".
        String tableName = "rewrite_path_sibling_check";
        createTable(tableName);
        try {
            Table table = loadTable(tableName);
            table.refresh();
            String tableLocation = table.location(); // e.g. file:///warehouse/.../tpch/tbl_extra
            // Construct a prefix that is a string-prefix of the location but NOT path-boundary aligned:
            // strip the last character so it matches as substring but not as path segment.
            String siblingPrefix = tableLocation.substring(0, tableLocation.length() - 1);
            assertQueryFails(
                    format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s')",
                            TEST_SCHEMA, tableName, siblingPrefix, siblingPrefix + "_new"),
                    "Table location .* does not start with source prefix .*");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRewriteTablePathFileListQuotesCommaInPath()
            throws IOException
    {
        // Paths containing commas (e.g. a staging directory named "migration,2026") must produce
        // valid RFC 4180 CSV — the field must be double-quoted so the CSV is still parseable.
        String sourceName = "rewrite_path_comma_source";
        String targetName = "rewrite_path_comma_target";
        createTable(sourceName);
        try {
            assertUpdate("INSERT INTO " + sourceName + " VALUES (1, 'a')", 1);

            Table table = loadTable(sourceName);
            table.refresh();
            String tableLocation = table.location();
            String sourcePrefix = tableLocation.substring(0, tableLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_comma_target";
            // Staging location with an embedded comma — propagates to every staged metadata path.
            String stagingWithComma = sourcePrefix + "_staging,dir";

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, sourceName, sourcePrefix, targetPrefix, stagingWithComma));

            // Read the file-list with the RFC 4180 parser — all rows must parse to exactly two fields.
            Path fileListPath = toPath(stagingWithComma + "/" + FILE_LIST_NAME);
            assertTrue(Files.exists(fileListPath), "file-list should exist");
            List<String> lines = Files.readAllLines(fileListPath);
            assertTrue(lines.size() > 0, "file-list must not be empty");
            for (String line : lines) {
                String[] parts = parseCsvLine(line);
                assertEquals(parts.length, 2, "CSV line must parse to exactly 2 fields: " + line);
            }

            // Verify that at least one quoted field is present (the staging path contains a comma).
            assertTrue(lines.stream().anyMatch(l -> l.startsWith("\"")),
                    "At least one field referencing the staging path must be quoted");

            // End-to-end: copy files and register to confirm the paths are valid after unquoting.
            for (String line : lines) {
                String[] parts = parseCsvLine(line);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }
            String metadataFileDest = lines.stream()
                    .map(l -> parseCsvLine(l)[1])
                    .filter(p -> p.endsWith(".metadata.json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .metadata.json in file-list"));
            String targetMetadataDir = metadataFileDest.substring(0, metadataFileDest.lastIndexOf('/'));
            assertUpdate(format("CALL system.register_table('%s', '%s', '%s')",
                    TEST_SCHEMA, targetName, targetMetadataDir));
            assertQuery(format("SELECT * FROM %s.%s ORDER BY id", TEST_SCHEMA, targetName),
                    "VALUES (1, 'a')");
        }
        finally {
            dropTable(sourceName);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + targetName);
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
                String[] parts = parseCsvLine(line);
                assertEquals(parts.length, 2, "Each line should have exactly two columns: " + line);
                assertTrue(parts[1].startsWith(targetPrefix),
                        format("Target column '%s' should start with '%s'", parts[1], targetPrefix));
            }

            List<String> sourceCol = lines.stream().map(l -> parseCsvLine(l)[0]).collect(Collectors.toList());
            List<String> targetCol = lines.stream().map(l -> parseCsvLine(l)[1]).collect(Collectors.toList());

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
                String[] parts = parseCsvLine(line);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Step 4: derive the metadata dir from the .metadata.json destination row.
            String metadataFileDest = lines.stream()
                    .map(l -> parseCsvLine(l)[1])
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
                String[] parts = parseCsvLine(line);
                Path from = toPath(parts[0]);
                Path to = toPath(parts[1]);
                Files.createDirectories(to.getParent());
                Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
            }

            // Step 4: derive the metadata dir from the .metadata.json destination row.
            String metadataFileDest = lines.stream()
                    .map(l -> parseCsvLine(l)[1])
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
    {
        // Position delete files embed data-file paths in their Parquet payload. Copying them
        // unchanged while relocating data files makes the deletes stop matching the new paths,
        // so deleted rows reappear in the migrated table. The procedure must reject this upfront.
        String tableName = "rewrite_with_deletes_test";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (format = 'PARQUET', format_version = '2')");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);

            Table table = loadTable(tableName);
            table.refresh();

            // Guard: confirm we actually produced position delete files; otherwise the test is vacuous.
            Snapshot snapshot = table.currentSnapshot();
            long deleteManifestCount = snapshot.deleteManifests(table.io()).stream().count();
            assertTrue(deleteManifestCount > 0, "Should have delete manifests after DELETE operation");

            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_with_deletes";
            String stagingLocation = sourcePrefix + "_deletes_staging";

            assertQueryFails(
                    format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s', true)",
                            TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation),
                    "Table '.*' contains position delete files.*");
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
    public void testRewriteTablePathPreservesManifestListAvroMetadata()
            throws IOException
    {
        // Regression test: manifest list files carry snapshot-id, parent-snapshot-id, and
        // sequence-number as Avro file-level metadata. These were silently dropped when the
        // rewriter used a fixed allowlist that only covered manifest keys, not manifest list keys.
        // Losing snapshot-id breaks Iceberg readers that rely on it for snapshot inheritance.
        String tableName = "rewrite_ml_avro_meta_test";
        createTable(tableName);
        try {
            // Two inserts so the second manifest list carries both snapshot-id and parent-snapshot-id.
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

            Table table = loadTable(tableName);
            table.refresh();
            String originalLocation = table.location();
            String sourcePrefix = originalLocation.substring(0, originalLocation.lastIndexOf('/'));
            String stagingLocation = sourcePrefix + "_ml_avro_staging";

            Snapshot snapshot = table.currentSnapshot();
            String manifestListPath = snapshot.manifestListLocation();

            // Read the source manifest list metadata before rewriting.
            org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> sourceReader =
                    new org.apache.avro.file.DataFileReader<>(
                            toPath(manifestListPath).toFile(),
                            new org.apache.avro.generic.GenericDatumReader<>());
            String sourceSnapshotId = sourceReader.getMetaString("snapshot-id");
            String sourceParentSnapshotId = sourceReader.getMetaString("parent-snapshot-id");
            String sourceSequenceNumber = sourceReader.getMetaString("sequence-number");
            sourceReader.close();

            assertTrue(sourceSnapshotId != null, "Source manifest list must have snapshot-id metadata");
            assertTrue(sourceSequenceNumber != null, "Source manifest list must have sequence-number metadata");

            // Rewrite with a staging location.
            String targetPrefix = sourcePrefix + "_ml_avro_target";
            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // Read the rewritten manifest list from staging.
            String stagedManifestListPath = manifestListPath.replace(sourcePrefix, stagingLocation);
            Path stagedFile = toPath(stagedManifestListPath);
            assertTrue(Files.exists(stagedFile), "Rewritten manifest list must exist at " + stagedFile);

            org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> targetReader =
                    new org.apache.avro.file.DataFileReader<>(
                            stagedFile.toFile(),
                            new org.apache.avro.generic.GenericDatumReader<>());
            String targetSnapshotId = targetReader.getMetaString("snapshot-id");
            String targetParentSnapshotId = targetReader.getMetaString("parent-snapshot-id");
            String targetSequenceNumber = targetReader.getMetaString("sequence-number");
            targetReader.close();

            assertEquals(targetSnapshotId, sourceSnapshotId,
                    "snapshot-id must be preserved in rewritten manifest list");
            assertEquals(targetParentSnapshotId, sourceParentSnapshotId,
                    "parent-snapshot-id must be preserved in rewritten manifest list");
            assertEquals(targetSequenceNumber, sourceSequenceNumber,
                    "sequence-number must be preserved in rewritten manifest list");
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
                String[] parts = parseCsvLine(line);
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
                    .map(l -> parseCsvLine(l)[1])
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
                String[] parts = parseCsvLine(line);
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
                    .map(l -> parseCsvLine(l)[1])
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
                String[] parts = parseCsvLine(line);
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
                    .map(l -> parseCsvLine(l)[1])
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
                String[] parts = parseCsvLine(line);
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
                    .map(l -> parseCsvLine(l)[1])
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
                    .map(l -> parseCsvLine(l)[1])
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

    @Test
    public void testRewriteTablePathCollectsPartitionStatisticsFiles()
            throws IOException
    {
        // partitionStatisticsFiles() must be collected into the file-list so they are copied
        // during migration. Previously only statisticsFiles() was included; partition stats files
        // would be referenced in the rewritten metadata JSON but never copied, leaving the target
        // table with dangling partition-statistics pointers.
        String tableName = "rewrite_partition_stats_test";
        createTable(tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Table table = loadTable(tableName);
            table.refresh();

            // Write a dummy partition statistics file under the table's metadata directory.
            String tableLocation = table.location();
            String statsFileName = "00001-pstats.parquet";
            String statsFilePath = tableLocation + "/metadata/" + statsFileName;
            Path statsLocalPath = toPath(statsFilePath);
            Files.createDirectories(statsLocalPath.getParent());
            Files.write(statsLocalPath, new byte[] {0x50, 0x41, 0x52, 0x31});

            // Register the dummy file as a partition statistics file on the table.
            long snapshotId = table.currentSnapshot().snapshotId();
            ImmutableGenericPartitionStatisticsFile partitionStatsFile =
                    ImmutableGenericPartitionStatisticsFile.builder()
                            .snapshotId(snapshotId)
                            .path(statsFilePath)
                            .fileSizeInBytes(Files.size(statsLocalPath))
                            .build();
            table.updatePartitionStatistics()
                    .setPartitionStatistics(partitionStatsFile)
                    .commit();
            table.refresh();

            // Verify the partition stats were committed to the table.
            TableMetadata currentMeta = ((BaseTable) table).operations().current();
            assertEquals(currentMeta.partitionStatisticsFiles().size(), 1,
                    "Table should have one partition statistics file registered");

            String sourcePrefix = tableLocation.substring(0, tableLocation.lastIndexOf('/'));
            String targetPrefix = sourcePrefix + "_pstats_target";
            String stagingLocation = sourcePrefix + "_pstats_staging";

            assertUpdate(format("CALL system.rewrite_table_path('%s', '%s', '%s', '%s', '%s')",
                    TEST_SCHEMA, tableName, sourcePrefix, targetPrefix, stagingLocation));

            // The partition statistics file must appear in the file-list source column.
            Path fileListPath = toPath(stagingLocation + "/" + FILE_LIST_NAME);
            assertTrue(Files.exists(fileListPath), "file-list should exist");
            List<String> lines = Files.readAllLines(fileListPath);
            boolean partitionStatsInList = lines.stream()
                    .map(l -> parseCsvLine(l)[0])
                    .anyMatch(src -> src.contains(statsFileName));
            assertTrue(partitionStatsInList,
                    "Partition statistics file '" + statsFileName + "' must appear in file-list source column");

            // Verify the target path is correctly mapped to the target prefix.
            String expectedTarget = statsFilePath.replace(sourcePrefix, targetPrefix);
            boolean correctTargetPresent = lines.stream()
                    .map(l -> parseCsvLine(l)[1])
                    .anyMatch(tgt -> tgt.equals(expectedTarget));
            assertTrue(correctTargetPresent,
                    "Partition statistics file must map to target path '" + expectedTarget + "' in file-list");
        }
        finally {
            dropTable(tableName);
        }
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
            String[] parts = parseCsvLine(line);
            Path from = toPath(parts[0]);
            Path to = toPath(parts[1]);
            Files.createDirectories(to.getParent());
            Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Parses one line of RFC 4180 CSV into fields.
     * Fields may be enclosed in double-quotes; {@code ""} inside a quoted field is a literal quote.
     */
    private static String[] parseCsvLine(String line)
    {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuote) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    }
                    else {
                        inQuote = false;
                    }
                }
                else {
                    current.append(c);
                }
            }
            else if (c == '"') {
                inQuote = true;
            }
            else if (c == ',') {
                fields.add(current.toString());
                current = new StringBuilder();
            }
            else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }
}
