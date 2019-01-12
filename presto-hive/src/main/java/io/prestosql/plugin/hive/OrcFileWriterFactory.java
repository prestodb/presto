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

import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.orc.OrcDataSink;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.OutputStreamOrcDataSink;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.OrcTableProperties;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterMaxDictionaryMemory;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeRows;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterMinStripeSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterValidateMode;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStringStatisticsLimit;
import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;

public class OrcFileWriterFactory
        implements HiveFileWriterFactory
{
    private final DateTimeZone hiveStorageTimeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats stats = new OrcWriterStats();
    private final OrcWriterOptions orcWriterOptions;

    @Inject
    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            HiveClientConfig hiveClientConfig,
            FileFormatDataSourceStats readStats,
            OrcFileWriterConfig config)
    {
        this(
                hdfsEnvironment,
                typeManager,
                nodeVersion,
                requireNonNull(hiveClientConfig, "hiveClientConfig is null").getDateTimeZone(),
                readStats,
                requireNonNull(config, "config is null").toOrcWriterOptions());
    }

    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            DateTimeZone hiveStorageTimeZone,
            FileFormatDataSourceStats readStats,
            OrcWriterOptions orcWriterOptions)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        this.readStats = requireNonNull(readStats, "stats is null");
        this.orcWriterOptions = requireNonNull(orcWriterOptions, "orcWriterOptions is null");
    }

    @Managed
    @Flatten
    public OrcWriterStats getStats()
    {
        return stats;
    }

    @Override
    public Optional<HiveFileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf configuration,
            ConnectorSession session)
    {
        if (!HiveSessionProperties.isOrcOptimizedWriterEnabled(session)) {
            return Optional.empty();
        }

        OrcEncoding orcEncoding;
        if (OrcOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            orcEncoding = ORC;
        }
        else if (com.facebook.hive.orc.OrcOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            orcEncoding = DWRF;
        }
        else {
            return Optional.empty();
        }

        CompressionKind compression = getCompression(schema, configuration, orcEncoding);

        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // an index to rearrange columns in the proper order
        List<String> fileColumnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMNS, ""));
        List<Type> fileColumnTypes = toHiveTypes(schema.getProperty(META_TABLE_COLUMN_TYPES, "")).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            OrcDataSink orcDataSink = createOrcDataSink(session, fileSystem, path);

            Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.empty();
            if (HiveSessionProperties.isOrcOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        return new HdfsOrcDataSource(
                                new OrcDataSourceId(path.toString()),
                                fileSystem.getFileStatus(path).getLen(),
                                getOrcMaxMergeDistance(session),
                                getOrcMaxBufferSize(session),
                                getOrcStreamBufferSize(session),
                                false,
                                fileSystem.open(path),
                                readStats);
                    }
                    catch (IOException e) {
                        throw new PrestoException(HIVE_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };

            return Optional.of(new OrcFileWriter(
                    orcDataSink,
                    rollbackAction,
                    orcEncoding,
                    fileColumnNames,
                    fileColumnTypes,
                    compression,
                    orcWriterOptions
                            .withStripeMinSize(getOrcOptimizedWriterMinStripeSize(session))
                            .withStripeMaxSize(getOrcOptimizedWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(getOrcOptimizedWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(getOrcOptimizedWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(getOrcStringStatisticsLimit(session)),
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(HiveMetadata.PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(HiveMetadata.PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    hiveStorageTimeZone,
                    validationInputFactory,
                    getOrcOptimizedWriterValidateMode(session),
                    stats));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating " + orcEncoding + " file", e);
        }
    }

    /**
     * Allow subclass to replace data sink implementation.
     */
    protected OrcDataSink createOrcDataSink(ConnectorSession session, FileSystem fileSystem, Path path)
            throws IOException
    {
        return new OutputStreamOrcDataSink(fileSystem.create(path));
    }

    private static CompressionKind getCompression(Properties schema, JobConf configuration, OrcEncoding orcEncoding)
    {
        String compressionName = schema.getProperty(OrcTableProperties.COMPRESSION.getPropName());
        if (compressionName == null) {
            compressionName = configuration.get("hive.exec.orc.default.compress");
        }
        if (compressionName == null) {
            return CompressionKind.ZLIB;
        }

        CompressionKind compression;
        try {
            compression = CompressionKind.valueOf(compressionName.toUpperCase(ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Unknown " + orcEncoding + " compression type " + compressionName);
        }
        return compression;
    }
}
