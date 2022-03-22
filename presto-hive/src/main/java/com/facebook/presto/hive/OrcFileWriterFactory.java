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

import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.datasink.DataSinkFactory;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfWriterEncryption;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.WriterEncryptionGroup;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.KeyProvider;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcConf;
import org.joda.time.DateTimeZone;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static com.facebook.presto.hive.HiveSessionProperties.getDwrfWriterStripeCacheeMaxSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterMaxDictionaryMemory;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeRows;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterMinStripeSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcOptimizedWriterValidateMode;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStringStatisticsLimit;
import static com.facebook.presto.hive.HiveSessionProperties.isDwrfWriterStripeCacheEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isExecutionBasedMemoryAccountingEnabled;
import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.metadata.KeyProvider.CRYPTO_SERVICE;
import static com.facebook.presto.orc.metadata.KeyProvider.UNKNOWN;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
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
    private final DataSinkFactory dataSinkFactory;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats stats = new OrcWriterStats();
    private final OrcFileWriterConfig orcFileWriterConfig;
    private final DwrfEncryptionProvider dwrfEncryptionProvider;

    @Inject
    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            DataSinkFactory dataSinkFactory,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            HiveClientConfig hiveClientConfig,
            FileFormatDataSourceStats readStats,
            OrcFileWriterConfig orcFileWriterConfig,
            HiveDwrfEncryptionProvider dwrfEncryptionProvider)
    {
        this(
                hdfsEnvironment,
                dataSinkFactory,
                typeManager,
                nodeVersion,
                requireNonNull(hiveClientConfig, "hiveClientConfig is null").getDateTimeZone(),
                readStats,
                orcFileWriterConfig,
                dwrfEncryptionProvider);
    }

    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            DataSinkFactory dataSinkFactory,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            DateTimeZone hiveStorageTimeZone,
            FileFormatDataSourceStats readStats,
            OrcFileWriterConfig orcFileWriterConfig,
            HiveDwrfEncryptionProvider dwrfEncryptionProvider)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.dataSinkFactory = requireNonNull(dataSinkFactory, "dataSinkFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        this.readStats = requireNonNull(readStats, "stats is null");
        this.orcFileWriterConfig = requireNonNull(orcFileWriterConfig, "orcFileWriterConfig is null");
        this.dwrfEncryptionProvider = requireNonNull(dwrfEncryptionProvider, "DwrfEncryptionProvider is null").toDwrfEncryptionProvider();
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
            ConnectorSession session,
            Optional<EncryptionInformation> encryptionInformation)
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
            DataSink dataSink = createDataSink(session, fileSystem, path);

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

            Optional<DwrfWriterEncryption> dwrfWriterEncryption = createDwrfEncryption(encryptionInformation, fileColumnNames, fileColumnTypes);

            return Optional.of(new OrcFileWriter(
                    dataSink,
                    rollbackAction,
                    orcEncoding,
                    fileColumnNames,
                    fileColumnTypes,
                    compression,
                    orcFileWriterConfig
                            .toOrcWriterOptionsBuilder()
                            .withStripeMinSize(getOrcOptimizedWriterMinStripeSize(session))
                            .withStripeMaxSize(getOrcOptimizedWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(getOrcOptimizedWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(getOrcOptimizedWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(getOrcStringStatisticsLimit(session))
                            .withIgnoreDictionaryRowGroupSizes(isExecutionBasedMemoryAccountingEnabled(session))
                            .withDwrfStripeCacheEnabled(isDwrfWriterStripeCacheEnabled(session))
                            .withDwrfStripeCacheMaxSize(getDwrfWriterStripeCacheeMaxSize(session))
                            .build(),
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(HiveMetadata.PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(MetastoreUtil.PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    hiveStorageTimeZone,
                    validationInputFactory,
                    getOrcOptimizedWriterValidateMode(session),
                    stats,
                    dwrfEncryptionProvider,
                    dwrfWriterEncryption));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating " + orcEncoding + " file. " + e.getMessage(), e);
        }
    }

    private Optional<DwrfWriterEncryption> createDwrfEncryption(Optional<EncryptionInformation> encryptionInformation, List<String> fileColumnNames, List<Type> types)
    {
        if (!encryptionInformation.isPresent()) {
            return Optional.empty();
        }

        if (!encryptionInformation.get().getDwrfEncryptionMetadata().isPresent()) {
            return Optional.empty();
        }

        DwrfEncryptionMetadata dwrfEncryptionMetadata = encryptionInformation.get().getDwrfEncryptionMetadata().get();

        List<OrcType> orcTypes = OrcType.createOrcRowType(0, fileColumnNames, types);
        Map<String, Integer> columnNamesMap = IntStream.range(0, fileColumnNames.size())
                .boxed()
                .collect(toImmutableMap(fileColumnNames::get, index -> index));

        Map<Integer, Slice> keyMap = dwrfEncryptionMetadata.toKeyMap(orcTypes, columnNamesMap);
        ImmutableListMultimap.Builder<Slice, Integer> encryptionGroupsBuilder = ImmutableListMultimap.builder();
        keyMap.entrySet().stream()
                .forEach(entry -> encryptionGroupsBuilder.put(entry.getValue(), entry.getKey()));
        ImmutableListMultimap<Slice, Integer> encryptionGroups = encryptionGroupsBuilder.build();

        List<WriterEncryptionGroup> writerEncryptionGroups = encryptionGroups.keySet().stream()
                .map(key -> new WriterEncryptionGroup(
                        encryptionGroups.get(key),
                        key))
                .collect(toImmutableList());
        return Optional.of(new DwrfWriterEncryption(
                toKeyProvider(dwrfEncryptionMetadata.getEncryptionProvider()),
                writerEncryptionGroups));
    }

    public KeyProvider toKeyProvider(String keyProviderName)
    {
        if (keyProviderName.toLowerCase(ENGLISH).equals("crypto")) {
            return CRYPTO_SERVICE;
        }
        return UNKNOWN;
    }

    public DataSink createDataSink(ConnectorSession session, FileSystem fileSystem, Path path)
            throws IOException
    {
        return dataSinkFactory.createDataSink(session, fileSystem, path);
    }

    private static CompressionKind getCompression(Properties schema, JobConf configuration, OrcEncoding orcEncoding)
    {
        String compressionName = OrcConf.COMPRESS.getString(schema, configuration);
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
