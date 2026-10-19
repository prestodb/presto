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

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.HdfsFileIO;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import javax.inject.Provider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

/**
 * Coordinator-only procedure that rewrites all Iceberg metadata files (table metadata JSON,
 * manifest list Avro files, and manifest Avro files) to a new location by substituting
 * {@code source_prefix} with {@code target_prefix} in every path string.
 *
 * <p>Data and delete files are NOT written. The original source files are left completely
 * untouched and the catalog is NOT updated. The caller should copy the data/delete files and
 * then call {@code system.register_table} pointing at the new metadata file.
 *
 * <p>When {@code staging_location} is provided, rewritten metadata files are physically written
 * under the staging directory (preserving the relative path suffix from the source). The internal
 * path strings embedded in each file still reference {@code target_prefix}, so the files are
 * ready to use once moved from staging to the final target. When omitted, files are written
 * to a default staging directory (UUID-named under the source metadata directory).
 *
 * <p>Every metadata JSON file the table still tracks is rewritten: all {@code previousFiles()}
 * entries plus the current metadata file. All data files reachable from the current metadata are
 * listed in the file list.
 *
 * <p>Rewrite scope per file type:
 * <ul>
 *   <li>Metadata JSON: full serialized JSON string replacement covers all path fields.</li>
 *   <li>Manifest list Avro: {@code manifest_path} and {@code manifest_length} fields (top-level)
 *       in each record.</li>
 *   <li>Manifest Avro: {@code data_file.file_path} field (nested under {@code data_file})
 *       in each record.</li>
 * </ul>
 *
 * <p><strong>Why {@code manifest_length} must be rewritten:</strong> rewriting a manifest changes
 * its size on disk — the embedded paths change length, and the Avro container is re-encoded with a
 * fresh sync marker and its own block framing. The {@code manifest_length} recorded in the manifest
 * list is the authoritative read bound for that manifest: {@code HdfsFileIO.newInputFile(ManifestFile)}
 * passes it to {@code HdfsInputFile}, whose {@code getLength()} returns it verbatim without ever
 * stat-ing the file. Iceberg then bounds the Avro read by that value
 * ({@code AvroIterable} → {@code AvroIO.stream(stream, file.getLength())}), and Presto's manifest
 * cache reads exactly that many bytes. A stale (too small) value therefore silently drops trailing
 * Avro blocks — manifest entries disappear with no error. Lengths are always recomputed from the
 * bytes actually written rather than derived from the prefix length difference.
 *
 * <p><strong>Partial failure:</strong> If an error occurs midway through rewriting, the staging
 * directory is left partially populated. A subsequent retry will overwrite conflicting files
 * deterministically (same inputs produce the same outputs), but the staging area will contain
 * a mix of files from both runs until the procedure completes successfully.
 */
public class RewriteTablePathProcedure
        implements Provider<Procedure>
{
    // visible for testing
    public static final String STAGING_DIR_PREFIX = "copy-table-staging-";
    public static final String FILE_LIST_NAME = "file-list";

    private static final String MANIFEST_DATA_FILE_FIELD = "data_file";
    private static final String MANIFEST_FILE_PATH_FIELD = "file_path";
    // V3 deletion-vector field: identifies the data file the deletion vector applies to.
    // Must be rewritten together with file_path so the DV still associates with the migrated file.
    private static final String MANIFEST_REFERENCED_DATA_FILE_FIELD = "referenced_data_file";
    private static final String MANIFEST_LIST_PATH_FIELD = "manifest_path";
    private static final String MANIFEST_LIST_LENGTH_FIELD = "manifest_length";
    // ManifestEntry.Status ordinal for a logically-deleted file. Files with this status remain in
    // the Avro container (needed for snapshot history) but may have been physically removed by
    // expire_snapshots / remove_orphan_files, so they must not be added to the copy file-list.
    private static final int MANIFEST_ENTRY_STATUS_DELETED = 2;

    private static final MethodHandle REWRITE_TABLE_PATH = methodHandle(
            RewriteTablePathProcedure.class,
            "rewriteTablePath",
            ConnectorSession.class,
            String.class,   // schema
            String.class,   // tableName
            String.class,   // sourcePrefix
            String.class,   // targetPrefix
            String.class,   // stagingLocation
            boolean.class); // createFileList

    private final IcebergMetadataFactory metadataFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final ManifestFileCache manifestFileCache;

    @Inject
    public RewriteTablePathProcedure(
            IcebergMetadataFactory metadataFactory,
            HdfsEnvironment hdfsEnvironment,
            ManifestFileCache manifestFileCache)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.manifestFileCache = requireNonNull(manifestFileCache, "manifestFileCache is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rewrite_table_path",
                ImmutableList.of(
                        new Argument("schema", VARCHAR),
                        new Argument("table_name", VARCHAR),
                        new Argument("source_prefix", VARCHAR),
                        new Argument("target_prefix", VARCHAR),
                        new Argument("staging_location", VARCHAR, false, null),
                        new Argument("create_file_list", BOOLEAN, false, true)),
                REWRITE_TABLE_PATH.bindTo(this));
    }

    public void rewriteTablePath(
            ConnectorSession session,
            String schema,
            String tableName,
            String sourcePrefix,
            String targetPrefix,
            String stagingLocation,
            boolean createFileList)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
            Table icebergTable = getIcebergTable(metadata, session, schemaTableName);

            if (sourcePrefix == null) {
                throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "source_prefix cannot be null");
            }
            if (targetPrefix == null) {
                throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "target_prefix cannot be null");
            }

            String normalizedSource = stripTrailingSlash(sourcePrefix);
            String normalizedTarget = stripTrailingSlash(targetPrefix);

            // Guard: source prefix must not be empty (would match between every character)
            if (normalizedSource.isEmpty()) {
                throw new PrestoException(INVALID_PROCEDURE_ARGUMENT,
                        "source_prefix cannot be empty or '/'");
            }

            // Guard: source and target must differ
            if (normalizedSource.equals(normalizedTarget)) {
                throw new PrestoException(INVALID_PROCEDURE_ARGUMENT,
                        "source_prefix and target_prefix must differ");
            }

            String currentLocation = icebergTable.location();
            // Verify table location is under source prefix (path-boundary aware)
            if (!hasPathPrefix(currentLocation, normalizedSource)) {
                throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, format(
                        "Table location '%s' does not start with source prefix '%s'",
                        currentLocation, normalizedSource));
            }

            TableMetadata currentMetadata = ((BaseTable) icebergTable).operations().current();

            // When staging_location is provided, metadata files are physically written there
            // (preserving the relative suffix from source_prefix). The embedded path strings
            // inside each file still reference target_prefix, so the files are correct once
            // moved from staging to the final target location.
            // When omitted, default to a UUID-named directory under the source table's
            // metadata directory (matching the Iceberg spec behaviour).
            String normalizedStaging = stagingLocation != null
                    ? stripTrailingSlash(stagingLocation)
                    : defaultStagingLocation(currentMetadata);

            // Guard: staging must differ from source to avoid overwriting source files.
            // Compare canonical forms so that equivalent URI spellings (file:/p vs file:///p,
            // dot segments like /p/./q) are recognised as the same location.
            if (canonicalizeLocation(normalizedStaging).equals(canonicalizeLocation(normalizedSource))) {
                throw new PrestoException(INVALID_PROCEDURE_ARGUMENT,
                        "staging_location must differ from source_prefix");
            }

            HdfsContext hdfsContext = new HdfsContext(session, schema, tableName, currentLocation, false);
            FileIO fileIO = new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext);

            // Reject before any write: position delete files embed data-file paths in their
            // Parquet payload. Copying them unchanged while relocating data files makes those
            // deletes stop matching the new paths, so deleted rows reappear in the migrated table.
            validateNoPositionDeleteFiles(currentMetadata, fileIO, schemaTableName);

            List<String[]> fileList = new ArrayList<>();

            List<String> allMetadataFiles = new ArrayList<>();
            for (TableMetadata.MetadataLogEntry entry : currentMetadata.previousFiles()) {
                allMetadataFiles.add(entry.file());
            }
            allMetadataFiles.add(currentMetadata.metadataFileLocation());

            Map<String, Long> rewrittenManifestLengths = rewriteManifests(
                    currentMetadata, fileIO, normalizedSource, normalizedTarget, normalizedStaging, fileList);
            rewriteManifestLists(
                    currentMetadata, fileIO, normalizedSource, normalizedTarget, normalizedStaging,
                    rewrittenManifestLengths, fileList);
            collectStatisticsFilePairs(currentMetadata, normalizedSource, normalizedTarget, fileList);
            rewriteAllMetadataJsonFiles(
                    allMetadataFiles, fileIO, normalizedSource, normalizedTarget, normalizedStaging, fileList);

            if (createFileList) {
                writeCsvFileList(fileList, normalizedStaging + "/" + FILE_LIST_NAME, fileIO);
            }
        }
    }

    /**
     * Returns the default staging directory: a UUID-named subdirectory under the source table's
     * metadata directory, matching the Iceberg spec default staging behaviour.
     *
     * <p><strong>Note:</strong> The default staging location is under the source prefix.
     * For cross-bucket migrations or when the source is read-only, the caller should explicitly
     * provide {@code staging_location} pointing to a writable location (typically under the
     * target prefix).
     */
    private static String defaultStagingLocation(TableMetadata sourceMetadata)
    {
        String metadataFileLocation = sourceMetadata.metadataFileLocation();
        String metadataDir = metadataFileLocation.substring(0, metadataFileLocation.lastIndexOf('/'));
        return metadataDir + "/" + STAGING_DIR_PREFIX + UUID.randomUUID();
    }

    /**
     * Rewrites each unique manifest Avro file across all snapshots. Manifests are shared across
     * snapshots so they are deduplicated by path. Data/delete file pairs are collected during the
     * rewrite (single I/O pass). Returns a map of source manifest path → rewritten byte length so
     * {@link #rewriteManifestLists} can correct {@code manifest_length} fields.
     */
    private static Map<String, Long> rewriteManifests(
            TableMetadata currentMetadata,
            FileIO fileIO,
            String normalizedSource,
            String normalizedTarget,
            String normalizedStaging,
            List<String[]> fileList)
    {
        Set<String> seen = new HashSet<>();
        Map<String, Long> rewrittenManifestLengths = new HashMap<>();
        for (Snapshot snapshot : currentMetadata.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(fileIO)) {
                if (seen.add(manifest.path())) {
                    if (!hasPathPrefix(manifest.path(), normalizedSource)) {
                        throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                                "Manifest path '%s' does not start with source_prefix '%s'",
                                manifest.path(), normalizedSource));
                    }
                    String stagingPath = normalizedStaging + manifest.path().substring(normalizedSource.length());
                    String finalPath = normalizedTarget + manifest.path().substring(normalizedSource.length());
                    long rewrittenLength = rewriteAvroFile(manifest.path(), MANIFEST_DATA_FILE_FIELD,
                            MANIFEST_FILE_PATH_FIELD, fileIO, normalizedSource, normalizedTarget,
                            stagingPath, fileList, null);
                    rewrittenManifestLengths.put(manifest.path(), rewrittenLength);
                    fileList.add(new String[] {stagingPath, finalPath});
                }
            }
        }
        return rewrittenManifestLengths;
    }

    /**
     * Rewrites manifest list Avro files for all snapshots, correcting each record's
     * {@code manifest_length} using the sizes returned by {@link #rewriteManifests}.
     */
    private static void rewriteManifestLists(
            TableMetadata currentMetadata,
            FileIO fileIO,
            String normalizedSource,
            String normalizedTarget,
            String normalizedStaging,
            Map<String, Long> rewrittenManifestLengths,
            List<String[]> fileList)
    {
        Set<String> seen = new HashSet<>();
        for (Snapshot snapshot : currentMetadata.snapshots()) {
            String manifestListPath = snapshot.manifestListLocation();
            if (manifestListPath != null && seen.add(manifestListPath)) {
                if (!hasPathPrefix(manifestListPath, normalizedSource)) {
                    throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                            "Manifest list path '%s' does not start with source_prefix '%s'",
                            manifestListPath, normalizedSource));
                }
                String stagingPath = normalizedStaging + manifestListPath.substring(normalizedSource.length());
                String finalPath = normalizedTarget + manifestListPath.substring(normalizedSource.length());
                rewriteAvroFile(manifestListPath, null, MANIFEST_LIST_PATH_FIELD, fileIO,
                        normalizedSource, normalizedTarget, stagingPath, null, rewrittenManifestLengths);
                fileList.add(new String[] {stagingPath, finalPath});
            }
        }
    }

    /**
     * Appends statistics file (.puffin/.stats) path pairs to {@code fileList} for both
     * {@code statistics} and {@code partition-statistics} entries. The metadata JSON rewrite
     * updates the embedded paths for both; this ensures the actual blobs are included in the
     * copy manifest so they are not missing at the target location.
     */
    private static void collectStatisticsFilePairs(
            TableMetadata currentMetadata,
            String normalizedSource,
            String normalizedTarget,
            List<String[]> fileList)
    {
        for (StatisticsFile statsFile : currentMetadata.statisticsFiles()) {
            String statsPath = statsFile.path();
            if (hasPathPrefix(statsPath, normalizedSource)) {
                fileList.add(new String[] {statsPath, normalizedTarget + statsPath.substring(normalizedSource.length())});
            }
        }
        for (PartitionStatisticsFile partStatsFile : currentMetadata.partitionStatisticsFiles()) {
            String statsPath = partStatsFile.path();
            if (hasPathPrefix(statsPath, normalizedSource)) {
                fileList.add(new String[] {statsPath, normalizedTarget + statsPath.substring(normalizedSource.length())});
            }
        }
    }

    /**
     * Rewrites every metadata JSON file the table tracks, skipping entries that fall outside the
     * source prefix (pre-migration log entries whose substring call would produce a garbled path).
     */
    private static void rewriteAllMetadataJsonFiles(
            List<String> allMetadataFiles,
            FileIO fileIO,
            String normalizedSource,
            String normalizedTarget,
            String normalizedStaging,
            List<String[]> fileList)
    {
        for (String metadataFile : allMetadataFiles) {
            if (!hasPathPrefix(metadataFile, normalizedSource)) {
                continue;
            }
            rewriteMetadataJson(metadataFile, fileIO, normalizedSource, normalizedTarget, normalizedStaging, fileList);
        }
    }

    /**
     * Writes a two-column RFC 4180 CSV (no header) of {@code source,target} file path pairs to
     * {@code path} using the given {@code fileIO}.
     *
     * <p>Fields containing commas, double-quotes, or line breaks are enclosed in double-quotes;
     * embedded double-quotes are escaped as {@code ""}.
     */
    private static void writeCsvFileList(List<String[]> fileList, String path, FileIO fileIO)
    {
        StringBuilder csv = new StringBuilder();
        for (String[] pair : fileList) {
            csv.append(csvQuote(pair[0])).append(',').append(csvQuote(pair[1])).append('\n');
        }
        byte[] bytes = csv.toString().getBytes(StandardCharsets.UTF_8);
        OutputFile outputFile = fileIO.newOutputFile(path);
        try (PositionOutputStream out = outputFile.createOrOverwrite()) {
            out.write(bytes);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to write file list to '%s'", path), e);
        }
    }

    private static String csvQuote(String value)
    {
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    /**
     * Reads the Avro container file at {@code sourcePath}, rewrites the string field identified
     * by {@code pathField} (optionally nested inside a record field named {@code nestedRecord}),
     * and writes the result to {@code writePath}. The embedded path strings are rewritten from
     * {@code sourcePrefix} to {@code targetPrefix} regardless of where the file is physically
     * written (supporting the staging-location pattern).
     *
     * <p>Optionally collects data file pairs while processing manifest files (when
     * {@code collectDataFiles} is non-null), avoiding a second I/O pass.
     *
     * @param nestedRecord if non-null, the path field is inside this nested record (e.g.
     *                     {@code "data_file"} for manifest files); if null the field is top-level
     *                     (e.g. manifest list files where {@code manifest_path} is top-level)
     * @param collectDataFiles if non-null, data file paths are collected into this list during
     *                         the rewrite pass (manifest files only)
     * @param manifestLengths if non-null, each record's {@code manifest_length} is replaced with the
     *                        size of the rewritten manifest, looked up by the record's original
     *                        {@code manifest_path} (manifest list files only)
     * @return the exact number of bytes written to {@code writePath}
     */
    // package-private for testing
    static long rewriteAvroFile(
            String sourcePath,
            String nestedRecord,
            String pathField,
            FileIO fileIO,
            String sourcePrefix,
            String targetPrefix,
            String writePath,
            List<String[]> collectDataFiles,
            Map<String, Long> manifestLengths)
    {
        InputFile inputFile = fileIO.newInputFile(sourcePath);
        // Stream input directly: wrap SeekableInputStream as SeekableInput so DataFileReader can
        // seek without reading the whole file into memory first.
        try (SeekableInputStream stream = inputFile.newStream()) {
            SeekableInput nonClosingInput = new SeekableInput()
            {
                @Override
                public void seek(long p) throws IOException
                {
                    stream.seek(p);
                }

                @Override
                public long tell() throws IOException
                {
                    return stream.getPos();
                }

                @Override
                public long length()
                {
                    return inputFile.getLength();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException
                {
                    return stream.read(b, off, len);
                }

                @Override
                public void close() throws IOException
                {
                    // stream lifecycle is managed by the enclosing try-with-resources
                }
            };
            try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                    nonClosingInput, new GenericDatumReader<>())) {
                Schema schema = reader.getSchema();
                OutputFile outputFile = fileIO.newOutputFile(writePath);
                // Stream output directly: DataFileWriter writes through a non-closing delegate so
                // rawOut stays open after writer.close(), letting us read getPos() for the exact
                // byte count without a second in-memory buffer or a separate stat call.
                try (PositionOutputStream rawOut = outputFile.createOrOverwrite()) {
                    PositionOutputStream writerOut = new PositionOutputStream()
                    {
                        @Override
                        public long getPos() throws IOException
                        {
                            return rawOut.getPos();
                        }

                        @Override
                        public void write(int b) throws IOException
                        {
                            rawOut.write(b);
                        }

                        @Override
                        public void write(byte[] b, int off, int len) throws IOException
                        {
                            rawOut.write(b, off, len);
                        }

                        @Override
                        public void flush() throws IOException
                        {
                            rawOut.flush();
                        }

                        @Override
                        public void close() throws IOException
                        {
                            // flush only; rawOut lifecycle is managed by the enclosing try-with-resources
                            rawOut.flush();
                        }
                    };
                    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                        String codec = reader.getMetaString("avro.codec");
                        writer.setCodec(CodecFactory.fromString(codec != null ? codec : "null"));

                        // Copy every non-avro.* metadata key from the source file.
                        // avro.codec is already applied above; avro.schema is written by writer.create().
                        // Non-avro keys include manifest keys (format-version, content, schema,
                        // partition-spec) and manifest-list keys (snapshot-id, parent-snapshot-id,
                        // sequence-number, first-row-id).
                        for (String key : reader.getMetaKeys()) {
                            if (!key.startsWith("avro.")) {
                                writer.setMeta(key, reader.getMeta(key));
                            }
                        }

                        writer.create(schema, writerOut);

                        // Collect file paths from both data and delete manifests
                        boolean shouldCollectFiles = collectDataFiles != null &&
                                MANIFEST_DATA_FILE_FIELD.equals(nestedRecord);

                        for (GenericRecord record : reader) {
                            // Collect data/delete file pairs if requested (manifest files only).
                            // status is a manifest-file field; it does not exist in manifest lists,
                            // so this entire block is guarded by shouldCollectFiles.
                            if (shouldCollectFiles) {
                                // Skip DELETED entries (status 2): they remain in the Avro for snapshot
                                // history but may have been physically removed by expire_snapshots or
                                // remove_orphan_files. Including them would break the copy step when the
                                // files no longer exist at the source.
                                Schema.Field statusField = schema.getField("status");
                                Object statusValue = statusField != null ? record.get(statusField.pos()) : null;
                                boolean isDeletedEntry = statusValue instanceof Integer &&
                                        (Integer) statusValue == MANIFEST_ENTRY_STATUS_DELETED;
                                if (!isDeletedEntry) {
                                    Schema.Field nestedField = schema.getField(nestedRecord);
                                    if (nestedField != null) {
                                        GenericRecord fileRecord = (GenericRecord) record.get(nestedField.pos());
                                        if (fileRecord != null) {
                                            Schema.Field filePathField = fileRecord.getSchema().getField(MANIFEST_FILE_PATH_FIELD);
                                            if (filePathField != null) {
                                                Object filePathValue = fileRecord.get(filePathField.pos());
                                                if (filePathValue != null) {
                                                    String filePath = filePathValue.toString();
                                                    if (!hasPathPrefix(filePath, sourcePrefix)) {
                                                        throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                                                                "Data file '%s' does not start with source_prefix '%s'. " +
                                                                "Tables with data files outside source_prefix (e.g. via " +
                                                                "write.data.path or add_files) cannot be migrated atomically; " +
                                                                "move those files under source_prefix first.",
                                                                filePath, sourcePrefix));
                                                    }
                                                    collectDataFiles.add(new String[] {
                                                            filePath,
                                                            targetPrefix + filePath.substring(sourcePrefix.length())
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Correct manifest_length before the path is rewritten, since the recorded
                            // lengths are keyed by the original (source) manifest path.
                            if (manifestLengths != null) {
                                updateManifestLength(record, manifestLengths);
                            }

                            // Always rewrite and write all records to the output manifest
                            rewritePathField(record, nestedRecord, pathField, sourcePrefix, targetPrefix);
                            // V3 deletion vectors: referenced_data_file must be kept in sync with
                            // file_path so the DV continues to associate with the relocated data file.
                            // rewritePathField is a no-op when the field is absent (pre-V3 schemas).
                            if (MANIFEST_DATA_FILE_FIELD.equals(nestedRecord)) {
                                rewritePathField(record, nestedRecord, MANIFEST_REFERENCED_DATA_FILE_FIELD, sourcePrefix, targetPrefix);
                            }
                            writer.append(record);
                        }
                    }
                    // writer.close() has flushed rawOut through writerOut; rawOut is still open.
                    return rawOut.getPos();
                }
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to rewrite Avro file '%s' -> '%s'", sourcePath, writePath), e);
        }
    }

    /**
     * Replaces the {@code manifest_length} of a manifest list record with the size of the
     * corresponding rewritten manifest, looked up by the record's current (still source-prefixed)
     * {@code manifest_path}. Must be called before {@code manifest_path} is rewritten.
     *
     * <p>If the manifest has no recorded length the field is left untouched, which keeps the record
     * self-consistent with a manifest that was not rewritten.
     */
    private static void updateManifestLength(GenericRecord record, Map<String, Long> manifestLengths)
    {
        Schema.Field pathField = record.getSchema().getField(MANIFEST_LIST_PATH_FIELD);
        Schema.Field lengthField = record.getSchema().getField(MANIFEST_LIST_LENGTH_FIELD);
        if (pathField == null || lengthField == null) {
            return;
        }
        Object pathValue = record.get(pathField.pos());
        if (pathValue == null) {
            return;
        }
        Long rewrittenLength = manifestLengths.get(pathValue.toString());
        if (rewrittenLength != null) {
            record.put(lengthField.pos(), rewrittenLength);
        }
    }

    /**
     * Rewrites the path string in {@code record.nestedRecord.pathField} (or
     * {@code record.pathField} when {@code nestedRecord} is null).
     */
    private static void rewritePathField(GenericRecord record, String nestedRecord, String pathField, String sourcePrefix, String targetPrefix)
    {
        GenericRecord target;
        if (nestedRecord != null) {
            // Check if the nested field exists in the schema before accessing
            Schema.Field nestedField = record.getSchema().getField(nestedRecord);
            if (nestedField == null) {
                return; // Field doesn't exist in this record's schema
            }
            target = (GenericRecord) record.get(nestedField.pos());
            if (target == null) {
                return;
            }
        }
        else {
            target = record;
        }

        Schema.Field field = target.getSchema().getField(pathField);
        if (field == null) {
            return;
        }
        Object value = target.get(field.pos());
        if (value != null) {
            String path = value.toString();
            String rewritten = hasPathPrefix(path, sourcePrefix)
                    ? targetPrefix + path.substring(sourcePrefix.length())
                    : path;
            target.put(field.pos(), new Utf8(rewritten));
        }
    }

    /**
     * Reads the Iceberg metadata JSON at {@code sourceMetadataPath}, rewrites the known path
     * fields from {@code sourcePrefix} to {@code targetPrefix}, and writes the result to
     * {@code stagingPath}.
     *
     * <p>Only the following Iceberg-defined path fields are rewritten; schema names, column
     * defaults, arbitrary property values, and any other non-path strings are left untouched:
     * <ul>
     *   <li>{@code location} — top-level table location</li>
     *   <li>{@code snapshots[*].manifest-list} — V2+ manifest-list pointer</li>
     *   <li>{@code snapshots[*].manifests[*]} — V1 inline manifest array</li>
     *   <li>{@code metadata-log[*].metadata-file} — previous metadata file pointer</li>
     *   <li>{@code statistics[*].statistics-path} — puffin statistics file path</li>
     *   <li>{@code partition-statistics[*].statistics-path} — partition statistics file path</li>
     *   <li>{@code properties} string values that are paths under the source prefix</li>
     * </ul>
     *
     * <p>Every path is rewritten only when {@link #hasPathPrefix} confirms the match is at a
     * path boundary, preventing {@code s3://bucket/tbl} from matching {@code s3://bucket/tbl_bak}.
     * Serialisation is handled by Jackson, so any characters in {@code targetPrefix} that require
     * JSON escaping (backslash, double-quote) are encoded correctly.
     *
     * <p>The staging path ({@code sourceMetadataPath} with {@code sourcePrefix} replaced by
     * {@code stagingPrefix}) is the physical write destination; the final target path (with
     * {@code targetPrefix}) is the logical destination. Both are appended to {@code fileList}.
     */
    private static void rewriteMetadataJson(
            String sourceMetadataPath,
            FileIO fileIO,
            String sourcePrefix,
            String targetPrefix,
            String stagingPrefix,
            List<String[]> fileList)
    {
        boolean gzip = isGzipMetadata(sourceMetadataPath);
        byte[] sourceBytes = readAllBytes(fileIO.newInputFile(sourceMetadataPath));
        String sourceJson = gzip ? gunzip(sourceBytes, sourceMetadataPath)
                                 : new String(sourceBytes, StandardCharsets.UTF_8);
        String rewrittenJson = rewriteMetadataJsonPaths(sourceJson, sourcePrefix, targetPrefix);

        // Compute staging path (physical write destination) and final target path (logical).
        // Anchored: sourceMetadataPath is validated to start with sourcePrefix by the caller.
        String stagingPath = stagingPrefix + sourceMetadataPath.substring(sourcePrefix.length());
        String finalTargetPath = targetPrefix + sourceMetadataPath.substring(sourcePrefix.length());

        byte[] rewrittenBytes = gzip ? gzip(rewrittenJson, stagingPath)
                                     : rewrittenJson.getBytes(StandardCharsets.UTF_8);
        OutputFile outputFile = fileIO.newOutputFile(stagingPath);
        try (PositionOutputStream out = outputFile.createOrOverwrite()) {
            out.write(rewrittenBytes);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to write rewritten metadata JSON to '%s'", stagingPath), e);
        }

        fileList.add(new String[] {stagingPath, finalTargetPath});
    }

    /**
     * Parses {@code json} as an Iceberg table-metadata document and rewrites every known path
     * field using an anchored prefix replacement. Only the fields listed in
     * {@link #rewriteMetadataJson} are touched; all other content is preserved verbatim.
     */
    static String rewriteMetadataJsonPaths(String json, String sourcePrefix, String targetPrefix)
    {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode root = (ObjectNode) mapper.readTree(json);

            // Top-level table location
            rewriteJsonStringField(root, "location", sourcePrefix, targetPrefix);

            // Per-snapshot paths
            for (JsonNode snapshot : jsonArrayOrEmpty(root.path("snapshots"))) {
                if (!snapshot.isObject()) {
                    continue;
                }
                ObjectNode snap = (ObjectNode) snapshot;
                // V2+: single manifest-list pointer
                rewriteJsonStringField(snap, "manifest-list", sourcePrefix, targetPrefix);
                // V1: inline manifests array (each element is a path string)
                rewriteJsonStringArray(snap, "manifests", sourcePrefix, targetPrefix);
            }

            // Metadata-log entries (previous metadata file locations)
            for (JsonNode entry : jsonArrayOrEmpty(root.path("metadata-log"))) {
                if (entry.isObject()) {
                    rewriteJsonStringField((ObjectNode) entry, "metadata-file", sourcePrefix, targetPrefix);
                }
            }

            // Puffin statistics files
            for (JsonNode statsFile : jsonArrayOrEmpty(root.path("statistics"))) {
                if (statsFile.isObject()) {
                    rewriteJsonStringField((ObjectNode) statsFile, "statistics-path", sourcePrefix, targetPrefix);
                }
            }

            // Partition statistics files
            for (JsonNode partStats : jsonArrayOrEmpty(root.path("partition-statistics"))) {
                if (partStats.isObject()) {
                    rewriteJsonStringField((ObjectNode) partStats, "statistics-path", sourcePrefix, targetPrefix);
                }
            }

            // Table properties whose values are paths under the source prefix
            // (e.g. write.location). Collect keys first to avoid ConcurrentModificationException.
            JsonNode propsNode = root.path("properties");
            if (propsNode.isObject()) {
                ObjectNode props = (ObjectNode) propsNode;
                List<String> pathKeys = new ArrayList<>();
                props.fields().forEachRemaining(e -> {
                    if (e.getValue().isTextual() && hasPathPrefix(e.getValue().textValue(), sourcePrefix)) {
                        pathKeys.add(e.getKey());
                    }
                });
                for (String key : pathKeys) {
                    String val = props.get(key).textValue();
                    props.put(key, targetPrefix + val.substring(sourcePrefix.length()));
                }
            }

            return mapper.writeValueAsString(root);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    "Failed to parse Iceberg metadata JSON for path rewriting", e);
        }
    }

    private static void rewriteJsonStringField(ObjectNode node, String field, String sourcePrefix, String targetPrefix)
    {
        JsonNode value = node.path(field);
        if (value.isTextual() && hasPathPrefix(value.textValue(), sourcePrefix)) {
            node.put(field, targetPrefix + value.textValue().substring(sourcePrefix.length()));
        }
    }

    private static void rewriteJsonStringArray(ObjectNode parent, String field, String sourcePrefix, String targetPrefix)
    {
        JsonNode node = parent.path(field);
        if (!node.isArray()) {
            return;
        }
        ArrayNode array = (ArrayNode) node;
        for (int i = 0; i < array.size(); i++) {
            JsonNode elem = array.get(i);
            if (elem.isTextual() && hasPathPrefix(elem.textValue(), sourcePrefix)) {
                array.set(i, array.textNode(targetPrefix + elem.textValue().substring(sourcePrefix.length())));
            }
        }
    }

    private static Iterable<JsonNode> jsonArrayOrEmpty(JsonNode node)
    {
        if (node == null || node.isMissingNode() || !node.isArray()) {
            return Collections.emptyList();
        }
        return node;
    }

    /**
     * Rejects the procedure early if the table's <em>current</em> snapshot contains any
     * position delete files.
     *
     * <p>Position delete files store the data-file path as a Parquet column value. That payload
     * is not rewritten by this procedure (only Avro metadata files are rewritten). After
     * relocation, data files live at new paths, but the position delete files still reference
     * the old paths, so the deletes no longer match and previously-deleted rows reappear.
     *
     * <p>Equality delete files are safe because they store column values, not file paths.
     *
     * <p><strong>Scope — current snapshot only:</strong> Historical snapshots that have already
     * been superseded by {@code rewrite_data_files} compaction may still reference position
     * delete files in the metadata log; those are not checked here. Tables that have been
     * compacted (so the current live view has no position deletes) may be migrated. Time-travel
     * to such historical snapshots after migration will produce incorrect results (deleted rows
     * may reappear) because the delete-file payloads are not rewritten; this is an accepted
     * limitation documented in the procedure contract.
     */
    private static void validateNoPositionDeleteFiles(
            TableMetadata currentMetadata,
            FileIO fileIO,
            SchemaTableName tableName)
    {
        Snapshot currentSnapshot = currentMetadata.currentSnapshot();
        if (currentSnapshot == null) {
            return; // empty table — no files of any kind
        }
        for (ManifestFile manifest : currentSnapshot.deleteManifests(fileIO)) {
            try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(
                    manifest, fileIO, currentMetadata.specsById())) {
                for (DeleteFile deleteFile : reader) {
                    if (deleteFile.content() == FileContent.POSITION_DELETES) {
                        throw new PrestoException(ICEBERG_INVALID_METADATA, format(
                                "Table '%s' contains position delete files. Position delete " +
                                "files store the data-file path as payload; those paths become " +
                                "stale after relocation and deleted rows reappear in the migrated " +
                                "table. Compact the table (e.g. rewrite_data_files) to remove " +
                                "position deletes before running rewrite_table_path.",
                                tableName));
                    }
                }
            }
            catch (IOException e) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, format(
                        "Failed to read delete manifest '%s'", manifest.path()), e);
            }
        }
    }

    /**
     * Returns true if {@code path} equals {@code prefix} exactly or begins with {@code prefix + "/"}.
     * Plain {@code startsWith} is not path-boundary aware: prefix {@code s3://b/tbl} would
     * incorrectly match {@code s3://b/tbl_backup/...}.
     */
    static boolean hasPathPrefix(String path, String prefix)
    {
        return path.equals(prefix) || path.startsWith(prefix + "/");
    }

    /**
     * Returns a canonical string form of {@code location} suitable for equality comparisons.
     *
     * <p>For {@code file:} URIs, resolves the local path to an absolute, normalised form so that
     * {@code file:/tmp/tbl}, {@code file:///tmp/tbl}, and {@code file:///tmp/tbl/.} all compare
     * equal. For all other schemes (s3, hdfs, …) {@link URI#normalize()} is applied, which
     * resolves dot-segments in the path component without requiring a filesystem call.
     *
     * <p>Falls back to the original string on any parse error so the caller's equality check
     * degrades gracefully to a plain string comparison.
     */
    static String canonicalizeLocation(String location)
    {
        URI uri;
        try {
            uri = URI.create(location).normalize();
        }
        catch (IllegalArgumentException e) {
            return location;
        }
        if ("file".equals(uri.getScheme())) {
            try {
                // Paths.get(URI) accepts both file:/path and file:///path and produces a
                // consistent java.nio.Path that toUri() re-serialises in a single canonical form.
                return Paths.get(uri).toAbsolutePath().normalize().toUri().toString();
            }
            catch (Exception e) {
                return uri.toString();
            }
        }
        return uri.toString();
    }

    private static byte[] readAllBytes(InputFile inputFile)
    {
        try (SeekableInputStream stream = inputFile.newStream()) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int read;
            while ((read = stream.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            return buffer.toByteArray();
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to read file '%s'", inputFile.location()), e);
        }
    }

    // Mirrors TableMetadataParser.Codec.fromFileName: .gz.metadata.json and .metadata.json.gz
    static boolean isGzipMetadata(String path)
    {
        if (path.endsWith(".metadata.json.gz")) {
            return true;
        }
        if (path.contains(".metadata.json")) {
            String withoutSuffix = path.substring(0, path.lastIndexOf(".metadata.json"));
            return withoutSuffix.endsWith(".gz");
        }
        return false;
    }

    private static String gunzip(byte[] compressed, String path)
    {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
            return out.toString(StandardCharsets.UTF_8.name());
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to decompress gzip metadata '%s'", path), e);
        }
    }

    private static byte[] gzip(String json, String path)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(json.getBytes(StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to gzip-compress rewritten metadata for '%s'", path), e);
        }
        return baos.toByteArray();
    }
}
