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
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFile;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
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
    private static final String MANIFEST_LIST_PATH_FIELD = "manifest_path";
    private static final String MANIFEST_LIST_LENGTH_FIELD = "manifest_length";

    private static final List<String> AVRO_METADATA_KEYS_TO_PRESERVE = ImmutableList.of(
            "format-version", "content", "partition-spec-id", "schema", "partition-spec");

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

            String normalizedSource = stripTrailingSlash(sourcePrefix);
            String normalizedTarget = stripTrailingSlash(targetPrefix);

            // Guard: source prefix must not be empty (would match between every character)
            if (normalizedSource.isEmpty()) {
                throw new PrestoException(ICEBERG_INVALID_METADATA,
                        "source_prefix cannot be empty or '/'");
            }

            // Guard: source and target must differ
            if (normalizedSource.equals(normalizedTarget)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA,
                        "source_prefix and target_prefix must differ");
            }

            String currentLocation = icebergTable.location();
            // Verify table location is under source prefix
            if (!currentLocation.startsWith(normalizedSource)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA, format(
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

            // Guard: staging must differ from source to avoid overwriting source files
            if (normalizedStaging.equals(normalizedSource)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA,
                        "staging_location must differ from source_prefix");
            }

            HdfsContext hdfsContext = new HdfsContext(session, schema, tableName, currentLocation, false);
            FileIO fileIO = new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext);

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
                    if (!manifest.path().startsWith(normalizedSource)) {
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
                if (!manifestListPath.startsWith(normalizedSource)) {
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
     * Appends statistics file (.puffin/.stats) path pairs to {@code fileList}. The metadata JSON
     * rewrite updates the embedded paths; this ensures the actual blobs are included in the copy
     * manifest. Statistics files are rarely generated in practice but handled for completeness.
     */
    private static void collectStatisticsFilePairs(
            TableMetadata currentMetadata,
            String normalizedSource,
            String normalizedTarget,
            List<String[]> fileList)
    {
        if (currentMetadata.statisticsFiles() == null) {
            return;
        }
        for (StatisticsFile statsFile : currentMetadata.statisticsFiles()) {
            String statsPath = statsFile.path();
            if (statsPath.startsWith(normalizedSource)) {
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
            if (!metadataFile.startsWith(normalizedSource)) {
                continue;
            }
            rewriteMetadataJson(metadataFile, fileIO, normalizedSource, normalizedTarget, normalizedStaging, fileList);
        }
    }

    /**
     * Writes a two-column CSV (no header) of {@code source,target} file path pairs to
     * {@code path} using the given {@code fileIO}.
     *
     * <p><strong>Note:</strong> Values are not quoted. This assumes paths do not contain commas,
     * which is true for S3/HDFS/GCS but may not hold for local filesystem paths or unusual configs.
     */
    private static void writeCsvFileList(List<String[]> fileList, String path, FileIO fileIO)
    {
        StringBuilder csv = new StringBuilder();
        for (String[] pair : fileList) {
            csv.append(pair[0]).append(',').append(pair[1]).append('\n');
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
    private static long rewriteAvroFile(
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
        byte[] sourceBytes = readAllBytes(fileIO.newInputFile(sourcePath));

        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(sourceBytes),
                new GenericDatumReader<>())) {
            Schema schema = reader.getSchema();
            // Buffer the rewritten container in memory rather than streaming straight to the
            // output file: the referencing manifest list must record this file's exact byte
            // length, which is only known once the Avro container is fully written and closed.
            // The source is already fully buffered above, so this adds no new memory class.
            ByteArrayOutputStream rewritten = new ByteArrayOutputStream(sourceBytes.length);
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                // Preserve codec
                String codec = reader.getMetaString("avro.codec");
                writer.setCodec(CodecFactory.fromString(codec != null ? codec : "null"));

                // Preserve specific Iceberg metadata keys that are safe to copy.
                // These include schema and partition-spec which are necessary for correct
                // interpretation of manifests when the table has undergone schema/partition evolution.
                // Avoid copying keys that may be location-specific or auto-generated.
                // Based on org.apache.iceberg.avro.AvroFileAppender metadata keys.
                for (String key : AVRO_METADATA_KEYS_TO_PRESERVE) {
                    byte[] value = reader.getMeta(key);
                    if (value != null) {
                        writer.setMeta(key, value);
                    }
                }

                writer.create(schema, rewritten);

                // Collect file paths from both data and delete manifests
                boolean shouldCollectFiles = collectDataFiles != null &&
                        MANIFEST_DATA_FILE_FIELD.equals(nestedRecord);

                for (GenericRecord record : reader) {
                    // Collect data/delete file pairs if requested (manifest files only)
                    if (shouldCollectFiles) {
                        // Check if the nested record field exists in the schema
                        Schema.Field nestedField = schema.getField(nestedRecord);
                        if (nestedField != null) {
                            GenericRecord fileRecord = (GenericRecord) record.get(nestedField.pos());
                            if (fileRecord != null) {
                                Schema.Field filePathField = fileRecord.getSchema().getField(MANIFEST_FILE_PATH_FIELD);
                                if (filePathField != null) {
                                    Object filePathValue = fileRecord.get(filePathField.pos());
                                    if (filePathValue != null) {
                                        String filePath = filePathValue.toString();
                                        if (filePath.startsWith(sourcePrefix)) {
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
                    writer.append(record);
                }
            }

            // The writer is closed at this point, so the Avro container is complete and the
            // buffered length is final.
            byte[] rewrittenBytes = rewritten.toByteArray();
            OutputFile outputFile = fileIO.newOutputFile(writePath);
            try (PositionOutputStream out = outputFile.createOrOverwrite()) {
                out.write(rewrittenBytes);
            }
            return rewrittenBytes.length;
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
            String rewritten = path.startsWith(sourcePrefix)
                    ? targetPrefix + path.substring(sourcePrefix.length())
                    : path;
            target.put(field.pos(), new Utf8(rewritten));
        }
    }

    /**
     * Reads the metadata JSON at {@code sourceMetadataPath}, rewrites all path strings from
     * {@code sourcePrefix} to {@code targetPrefix}, and physically writes the raw rewritten
     * bytes directly to {@code stagingPath}. Writing the raw bytes (rather than re-serialising
     * through {@code TableMetadataParser.write}) ensures the file exactly mirrors what the
     * caller requested — in particular, {@code TableMetadataParser.write} would embed the
     * {@code OutputFile.location()} (i.e. the staging path) as the {@code metadataFileLocation}
     * field, which would put the staging path back into the content.
     *
     * <p>The staging path is computed as:
     * {@code sourceMetadataPath.replace(sourcePrefix, stagingPrefix)}.
     * The final target path is:
     * {@code sourceMetadataPath.replace(sourcePrefix, targetPrefix)}.
     *
     * <p>Both paths are appended as a {@code [stagingPath, finalTargetPath]} row to
     * {@code fileList}.
     *
     * <p><strong>Limitation:</strong> Uses unanchored {@code String.replace(sourcePrefix, targetPrefix)}
     * over the entire JSON document. This rewrites all occurrences of {@code sourcePrefix}, including:
     * <ul>
     *   <li>Intended path fields: {@code location}, {@code metadata-file-location}, {@code manifest-list},
     *       {@code manifest-path}, {@code statistics}, snapshot manifest lists, etc.</li>
     *   <li>Unintended non-path fields: table properties, column comments, partition spec defaults,
     *       or any user-supplied metadata that happens to contain the prefix as a substring.</li>
     * </ul>
     * For whole-bucket migrations (e.g., {@code s3://source-bucket → s3://target-bucket}) this is typically
     * correct. For parent-directory prefixes (e.g., {@code s3://bucket/warehouse → s3://bucket/warehouse2})
     * embedded in property values, this may rewrite unintended fields. Alternative: parse JSON and rewrite
     * only known path fields (as Iceberg's Spark RewriteTablePathAction does). This implementation prioritizes
     * simplicity and handles the common case correctly.
     */
    private static void rewriteMetadataJson(
            String sourceMetadataPath,
            FileIO fileIO,
            String sourcePrefix,
            String targetPrefix,
            String stagingPrefix,
            List<String[]> fileList)
    {
        // Read and rewrite the raw JSON text — a single string replace covers all path fields
        // (table location, metadataFileLocation, snapshot manifest-list pointers,
        // metadata-log entries, statistics file paths, etc.).
        byte[] sourceBytes = readAllBytes(fileIO.newInputFile(sourceMetadataPath));
        String rewrittenJson = new String(sourceBytes, StandardCharsets.UTF_8)
                .replace(sourcePrefix, targetPrefix);

        // Compute staging path (physical write destination) and final target path (logical).
        // Anchored: sourceMetadataPath is validated to start with sourcePrefix by the caller.
        String stagingPath = stagingPrefix + sourceMetadataPath.substring(sourcePrefix.length());
        String finalTargetPath = targetPrefix + sourceMetadataPath.substring(sourcePrefix.length());

        // Write the rewritten bytes directly — bypassing TableMetadataParser.write() which
        // would re-serialise the object and embed the staging OutputFile.location() as the
        // metadataFileLocation, thereby reintroducing the staging path into the content.
        byte[] rewrittenBytes = rewrittenJson.getBytes(StandardCharsets.UTF_8);
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
}
